"""SQLite-backed immutable span intake, tail sampling, and trace queries."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .analysis import analyze_trace, percentile
from .errors import ConflictError, NotFoundError, StorageError, ValidationError
from .model import Span, validate_trace_id

SCHEMA_VERSION = 1
DEFAULT_MAX_ACTIVE_TRACES = 10_000
DEFAULT_MAX_SPANS_PER_TRACE = 1_000
MAX_BATCH_SPANS = 100


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def deterministic_sample(trace_id: str, rate: float) -> bool:
    """Make a stable probability decision from a trace ID."""

    validate_trace_id(trace_id)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)):
        raise ValidationError("sample rate must be numeric")
    rate = float(rate)
    if not math.isfinite(rate) or not 0 <= rate <= 1:
        raise ValidationError("sample rate must be between zero and one")
    bucket = int(hashlib.sha256(trace_id.encode("ascii")).hexdigest()[:16], 16)
    return bucket < int(rate * 2**64)


class TraceStore:
    """Durable collector state with idempotent intake and delayed sampling decisions."""

    def __init__(
        self,
        database: str | os.PathLike[str],
        *,
        max_active_traces: int = DEFAULT_MAX_ACTIVE_TRACES,
        max_spans_per_trace: int = DEFAULT_MAX_SPANS_PER_TRACE,
    ):
        if not isinstance(max_active_traces, int) or isinstance(max_active_traces, bool):
            raise ValidationError("max_active_traces must be an integer")
        if not 1 <= max_active_traces <= 1_000_000:
            raise ValidationError("max_active_traces is outside the supported range")
        if not isinstance(max_spans_per_trace, int) or isinstance(max_spans_per_trace, bool):
            raise ValidationError("max_spans_per_trace must be an integer")
        if not 1 <= max_spans_per_trace <= 100_000:
            raise ValidationError("max_spans_per_trace is outside the supported range")
        candidate = Path(database).expanduser().absolute()
        if candidate.is_symlink():
            raise ValidationError("database path cannot be a symlink")
        candidate.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if candidate.parent.is_symlink():
            raise ValidationError("database parent cannot be a symlink")
        self.database = candidate
        self.max_active_traces = max_active_traces
        self.max_spans_per_trace = max_spans_per_trace
        self._initialize()

    def ingest(self, span: Span, *, received_ns: int | None = None) -> dict[str, Any]:
        return self.ingest_many([span], received_ns=received_ns)

    def ingest_many(
        self,
        spans: Iterable[Span],
        *,
        received_ns: int | None = None,
    ) -> dict[str, Any]:
        span_list = list(spans)
        if not 1 <= len(span_list) <= MAX_BATCH_SPANS:
            raise ValidationError(f"a batch must contain 1 to {MAX_BATCH_SPANS} spans")
        if any(not isinstance(span, Span) for span in span_list):
            raise ValidationError("ingest_many accepts only validated Span objects")
        timestamp = time.time_ns() if received_ns is None else received_ns
        if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp < 0:
            raise ValidationError("received_ns must be a non-negative integer")
        accepted = 0
        duplicates = 0
        trace_ids: set[str] = set()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM traces WHERE state = 'active'"
                ).fetchone()[0]
            )
            new_trace_ids: set[str] = set()
            pending_counts: dict[str, int] = {}
            for span in span_list:
                trace_ids.add(span.trace_id)
                row = connection.execute(
                    "SELECT state, span_count FROM traces WHERE trace_id = ?",
                    (span.trace_id,),
                ).fetchone()
                if row is None:
                    if span.trace_id not in new_trace_ids:
                        if active_count + len(new_trace_ids) >= self.max_active_traces:
                            raise StorageError("active trace capacity is exhausted")
                        connection.execute(
                            """
                            INSERT INTO traces(
                                trace_id, state, first_received_ns, last_received_ns, span_count
                            ) VALUES (?, 'active', ?, ?, 0)
                            """,
                            (span.trace_id, timestamp, timestamp),
                        )
                        new_trace_ids.add(span.trace_id)
                    state = "active"
                    persisted_count = 0
                else:
                    state = str(row["state"])
                    persisted_count = int(row["span_count"])
                if state != "active":
                    raise ConflictError(f"trace is already finalized: {span.trace_id}")

                payload = canonical_json(span.to_dict())
                payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                existing = connection.execute(
                    "SELECT payload_hash FROM spans WHERE trace_id = ? AND span_id = ?",
                    (span.trace_id, span.span_id),
                ).fetchone()
                if existing is not None:
                    if existing["payload_hash"] != payload_hash:
                        raise ConflictError(
                            f"span id was reused with different data: {span.span_id}"
                        )
                    duplicates += 1
                    continue
                count = pending_counts.get(span.trace_id, persisted_count)
                if count >= self.max_spans_per_trace:
                    raise StorageError(
                        f"span limit reached for trace: {span.trace_id}"
                    )
                connection.execute(
                    """
                    INSERT INTO spans(
                        trace_id, span_id, parent_span_id, service, name, kind,
                        start_ns, duration_ns, status, attributes_json, payload_hash, received_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        span.trace_id,
                        span.span_id,
                        span.parent_span_id,
                        span.service,
                        span.name,
                        span.kind,
                        span.start_ns,
                        span.duration_ns,
                        span.status,
                        canonical_json(span.attributes),
                        payload_hash,
                        timestamp,
                    ),
                )
                pending_counts[span.trace_id] = count + 1
                accepted += 1
            for trace_id, count in pending_counts.items():
                connection.execute(
                    "UPDATE traces SET span_count = ?, last_received_ns = ? WHERE trace_id = ?",
                    (count, timestamp, trace_id),
                )
        return {
            "accepted": accepted,
            "duplicates": duplicates,
            "trace_ids": sorted(trace_ids),
        }

    def finalize(
        self,
        trace_id: str,
        *,
        slow_threshold_ns: int = 100_000_000,
        normal_sample_rate: float = 0.1,
        finalized_ns: int | None = None,
    ) -> dict[str, Any]:
        trace_id = validate_trace_id(trace_id)
        if (
            not isinstance(slow_threshold_ns, int)
            or isinstance(slow_threshold_ns, bool)
            or slow_threshold_ns < 0
        ):
            raise ValidationError("slow_threshold_ns must be a non-negative integer")
        # Validate even when an error/slow decision would bypass probability sampling.
        deterministic_sample(trace_id, normal_sample_rate)
        timestamp = time.time_ns() if finalized_ns is None else finalized_ns
        if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp < 0:
            raise ValidationError("finalized_ns must be a non-negative integer")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute(
                "SELECT state, report_json FROM traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            if state is None:
                raise NotFoundError(f"trace not found: {trace_id}")
            if state["state"] != "active":
                report = json.loads(state["report_json"])
                report["already_finalized"] = True
                return report
            spans = [
                self._span_from_row(row)
                for row in connection.execute(
                    "SELECT * FROM spans WHERE trace_id = ? ORDER BY start_ns, span_id",
                    (trace_id,),
                )
            ]
            report = analyze_trace(spans)
            if report["error_count"]:
                reason = "error"
                keep = True
            elif report["root_duration_ns"] >= slow_threshold_ns:
                reason = "slow"
                keep = True
            elif deterministic_sample(trace_id, normal_sample_rate):
                reason = "probabilistic"
                keep = True
            else:
                reason = "not_sampled"
                keep = False
            report.update(
                {
                    "already_finalized": False,
                    "finalized_ns": timestamp,
                    "kept": keep,
                    "normal_sample_rate": float(normal_sample_rate),
                    "sampling_reason": reason,
                    "slow_threshold_ns": slow_threshold_ns,
                }
            )
            report_json = canonical_json(report)
            connection.execute(
                """
                UPDATE traces
                SET state = ?, decision_reason = ?, finalized_ns = ?,
                    root_duration_ns = ?, error_count = ?, bottleneck_service = ?,
                    report_json = ?
                WHERE trace_id = ?
                """,
                (
                    "kept" if keep else "dropped",
                    reason,
                    timestamp,
                    report["root_duration_ns"],
                    report["error_count"],
                    report["bottleneck_service"],
                    report_json,
                    trace_id,
                ),
            )
            if not keep:
                connection.execute("DELETE FROM spans WHERE trace_id = ?", (trace_id,))
        return report

    def get_trace(self, trace_id: str) -> dict[str, Any]:
        trace_id = validate_trace_id(trace_id)
        with self._connect() as connection:
            state = connection.execute(
                "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            if state is None:
                raise NotFoundError(f"trace not found: {trace_id}")
            spans = [
                self._span_from_row(row).to_dict()
                for row in connection.execute(
                    "SELECT * FROM spans WHERE trace_id = ? ORDER BY start_ns, span_id",
                    (trace_id,),
                )
            ]
        return {
            "report": json.loads(state["report_json"]) if state["report_json"] else None,
            "spans": spans,
            "state": state["state"],
            "stored_span_count": len(spans),
            "trace_id": trace_id,
        }

    def overview(self) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT state, decision_reason, span_count, root_duration_ns,
                       error_count, bottleneck_service
                FROM traces WHERE state != 'active' ORDER BY trace_id
                """
            ).fetchall()
            active = int(
                connection.execute(
                    "SELECT COUNT(*) FROM traces WHERE state = 'active'"
                ).fetchone()[0]
            )
        durations = [int(row["root_duration_ns"]) for row in rows]
        kept = sum(row["state"] == "kept" for row in rows)
        observed_spans = sum(int(row["span_count"]) for row in rows)
        retained_spans = sum(
            int(row["span_count"]) for row in rows if row["state"] == "kept"
        )
        reasons: dict[str, int] = {}
        bottlenecks: dict[str, int] = {}
        for row in rows:
            reasons[row["decision_reason"]] = reasons.get(row["decision_reason"], 0) + 1
            service = row["bottleneck_service"]
            bottlenecks[service] = bottlenecks.get(service, 0) + 1
        return {
            "active_traces": active,
            "bottleneck_counts": dict(sorted(bottlenecks.items())),
            "dropped_traces": len(rows) - kept,
            "error_traces": sum(int(row["error_count"]) > 0 for row in rows),
            "finalized_traces": len(rows),
            "kept_traces": kept,
            "latency_ms": {
                "p50": self._ns_to_ms(percentile(durations, 0.50)),
                "p95": self._ns_to_ms(percentile(durations, 0.95)),
                "p99": self._ns_to_ms(percentile(durations, 0.99)),
            },
            "observed_spans": observed_spans,
            "retained_spans": retained_spans,
            "sampling_reasons": dict(sorted(reasons.items())),
            "span_storage_reduction_percent": round(
                (observed_spans - retained_spans) * 100 / observed_spans, 2
            )
            if observed_spans
            else 0.0,
        }

    def service_graph(self, *, kept_only: bool = True) -> dict[str, Any]:
        condition = "state = 'kept'" if kept_only else "state != 'active'"
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT report_json FROM traces WHERE {condition} ORDER BY trace_id"
            ).fetchall()
        edges: dict[tuple[str, str], int] = defaultdict(int)
        services: set[str] = set()
        for row in rows:
            report = json.loads(row["report_json"])
            services.update(report["services"])
            for edge in report["dependency_edges"]:
                edges[(edge["source"], edge["target"])] += int(edge["calls"])
        return {
            "edges": [
                {"calls": calls, "source": source, "target": target}
                for (source, target), calls in sorted(edges.items())
            ],
            "services": sorted(services),
            "trace_count": len(rows),
        }

    def purge_stale(self, *, before_received_ns: int) -> dict[str, int]:
        if (
            not isinstance(before_received_ns, int)
            or isinstance(before_received_ns, bool)
            or before_received_ns < 0
        ):
            raise ValidationError("before_received_ns must be a non-negative integer")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            ids = [
                row[0]
                for row in connection.execute(
                    """
                    SELECT trace_id FROM traces
                    WHERE state = 'active' AND last_received_ns < ?
                    """,
                    (before_received_ns,),
                )
            ]
            removed_spans = 0
            for trace_id in ids:
                removed_spans += connection.execute(
                    "DELETE FROM spans WHERE trace_id = ?", (trace_id,)
                ).rowcount
                connection.execute("DELETE FROM traces WHERE trace_id = ?", (trace_id,))
        return {"purged_spans": removed_spans, "purged_traces": len(ids)}

    def _initialize(self) -> None:
        connection = sqlite3.connect(self.database)
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS traces (
                    trace_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (state IN ('active', 'kept', 'dropped')),
                    first_received_ns INTEGER NOT NULL,
                    last_received_ns INTEGER NOT NULL,
                    span_count INTEGER NOT NULL CHECK (span_count >= 0),
                    decision_reason TEXT,
                    finalized_ns INTEGER,
                    root_duration_ns INTEGER,
                    error_count INTEGER,
                    bottleneck_service TEXT,
                    report_json TEXT
                );
                CREATE TABLE IF NOT EXISTS spans (
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT,
                    service TEXT NOT NULL,
                    name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    start_ns INTEGER NOT NULL,
                    duration_ns INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    attributes_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    received_ns INTEGER NOT NULL,
                    PRIMARY KEY(trace_id, span_id),
                    FOREIGN KEY(trace_id) REFERENCES traces(trace_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_spans_trace_start
                    ON spans(trace_id, start_ns, span_id);
                CREATE INDEX IF NOT EXISTS idx_traces_state
                    ON traces(state, finalized_ns);
                """
            )
            existing = connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(existing[0]) != SCHEMA_VERSION:
                raise StorageError("database schema version is unsupported")
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.database, 0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _span_from_row(row: sqlite3.Row) -> Span:
        return Span(
            trace_id=row["trace_id"],
            span_id=row["span_id"],
            parent_span_id=row["parent_span_id"],
            service=row["service"],
            name=row["name"],
            kind=row["kind"],
            start_ns=int(row["start_ns"]),
            duration_ns=int(row["duration_ns"]),
            status=row["status"],
            attributes=json.loads(row["attributes_json"]),
        )

    @staticmethod
    def _ns_to_ms(value: float | None) -> float | None:
        return None if value is None else round(value / 1_000_000, 3)
