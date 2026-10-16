"""Deterministic tail-sampling and latency-diagnosis experiment."""

from __future__ import annotations

import hashlib
import random
import tempfile
import time
from pathlib import Path
from typing import Any

from .errors import ConflictError, ValidationError
from .model import SPAN_FIELDS, Span
from .store import TraceStore, deterministic_sample

TRACE_COUNT = 800
NORMAL_SAMPLE_RATE = 0.10
SLOW_THRESHOLD_NS = 80_000_000


def _hex_id(label: str, length: int) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()[:length]


def synthetic_trace(index: int) -> tuple[list[Span], bool, bool]:
    trace_id = _hex_id(f"tracelens-trace-{index}", 32)
    base = 1_800_000_000_000_000_000 + index * 1_000_000_000
    slow = index % 16 == 0
    failed = index % 40 == 0
    catalog_duration = 95_000_000 if slow else 15_000_000
    inventory_duration = 88_000_000 if slow else 8_000_000
    payment_start = 7_000_000 + catalog_duration
    root_duration = payment_start + 10_000_000

    def make(
        label: str,
        parent: str | None,
        service: str,
        name: str,
        kind: str,
        start_offset: int,
        duration: int,
        status: str = "OK",
        attributes: dict[str, str | int | float | bool] | None = None,
    ) -> Span:
        return Span(
            trace_id=trace_id,
            span_id=_hex_id(f"{trace_id}:{label}", 16),
            parent_span_id=parent,
            service=service,
            name=name,
            kind=kind,
            start_ns=base + start_offset,
            duration_ns=duration,
            status=status,
            attributes=attributes or {"synthetic": True},
        )

    root_id = _hex_id(f"{trace_id}:root", 16)
    catalog_id = _hex_id(f"{trace_id}:catalog", 16)
    spans = [
        make(
            "root",
            None,
            "gateway",
            "POST /checkout",
            "SERVER",
            0,
            root_duration,
            "ERROR" if failed else "OK",
            {"http.route": "/checkout", "synthetic": True},
        ),
        make("identity", root_id, "identity", "verify-session", "CLIENT", 1_000_000, 3_000_000),
        make(
            "catalog",
            root_id,
            "catalog",
            "load-cart",
            "CLIENT",
            5_000_000,
            catalog_duration,
        ),
        make(
            "inventory",
            catalog_id,
            "inventory",
            "reserve-stock",
            "CLIENT",
            7_000_000,
            inventory_duration,
        ),
        make(
            "pricing",
            catalog_id,
            "pricing",
            "price-cart",
            "CLIENT",
            5_000_000 + catalog_duration - 4_000_000,
            3_000_000,
        ),
        make(
            "payment",
            root_id,
            "payments",
            "authorize",
            "CLIENT",
            payment_start,
            8_000_000,
            "ERROR" if failed else "OK",
            {"payment.test_decline": failed, "synthetic": True},
        ),
    ]
    random.Random(index).shuffle(spans)
    return spans, slow, failed


def _invalid_payload_rejections() -> int:
    valid = synthetic_trace(1)[0][0].to_dict()
    invalid: list[dict[str, Any]] = []
    missing = dict(valid)
    missing.pop("trace_id")
    invalid.append(missing)
    zero_trace = dict(valid, trace_id="0" * 32)
    invalid.append(zero_trace)
    bad_parent = dict(valid, parent_span_id="not-hex")
    invalid.append(bad_parent)
    negative_duration = dict(valid, duration_ns=-1)
    invalid.append(negative_duration)
    unknown = dict(valid, unexpected=True)
    invalid.append(unknown)
    rejected = 0
    for payload in invalid:
        try:
            if set(payload) != SPAN_FIELDS:
                Span.from_dict(payload)
            else:
                Span.from_dict(payload)
        except ValidationError:
            rejected += 1
    return rejected


def _run(database: Path) -> dict[str, Any]:
    started = time.perf_counter()
    store = TraceStore(database, max_active_traces=1_000, max_spans_per_trace=20)
    duplicate_deliveries = 0
    conflicting_duplicates_rejected = 0
    anomalous = 0
    head_anomalies_kept = 0
    tail_anomalies_kept = 0
    slow_traces = 0
    error_traces = 0
    slow_bottleneck_correct = 0
    first_trace_id = ""

    for index in range(TRACE_COUNT):
        spans, slow, failed = synthetic_trace(index)
        trace_id = spans[0].trace_id
        first_trace_id = first_trace_id or trace_id
        store.ingest_many(spans, received_ns=2_000_000_000_000_000_000 + index)
        if index % 10 == 0:
            duplicate_deliveries += store.ingest(
                spans[0], received_ns=2_000_000_000_000_001_000 + index
            )["duplicates"]
        if index == 0:
            altered = spans[0].to_dict()
            altered["duration_ns"] += 1
            try:
                store.ingest(Span.from_dict(altered))
            except ConflictError:
                conflicting_duplicates_rejected += 1

        is_anomalous = slow or failed
        anomalous += is_anomalous
        slow_traces += slow
        error_traces += failed
        if is_anomalous and deterministic_sample(trace_id, NORMAL_SAMPLE_RATE):
            head_anomalies_kept += 1
        report = store.finalize(
            trace_id,
            slow_threshold_ns=SLOW_THRESHOLD_NS,
            normal_sample_rate=NORMAL_SAMPLE_RATE,
            finalized_ns=2_100_000_000_000_000_000 + index,
        )
        if is_anomalous and report["kept"]:
            tail_anomalies_kept += 1
        if slow and report["bottleneck_service"] == "inventory":
            slow_bottleneck_correct += 1

    repeated_finalize = store.finalize(
        first_trace_id,
        slow_threshold_ns=SLOW_THRESHOLD_NS,
        normal_sample_rate=NORMAL_SAMPLE_RATE,
    )
    overview = store.overview()
    graph = store.service_graph()
    invalid_rejections = _invalid_payload_rejections()
    success = all(
        (
            overview["finalized_traces"] == TRACE_COUNT,
            overview["active_traces"] == 0,
            tail_anomalies_kept == anomalous,
            slow_bottleneck_correct == slow_traces,
            duplicate_deliveries == TRACE_COUNT // 10,
            conflicting_duplicates_rejected == 1,
            invalid_rejections == 5,
            repeated_finalize["already_finalized"],
            len(graph["services"]) == 6,
        )
    )
    return {
        "analysis": {
            "slow_bottleneck_accuracy_percent": round(
                slow_bottleneck_correct * 100 / slow_traces, 2
            ),
            "slow_bottleneck_correct": slow_bottleneck_correct,
            "slow_traces": slow_traces,
            "topology_edges": len(graph["edges"]),
            "topology_services": len(graph["services"]),
        },
        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        "ingestion": {
            "conflicting_duplicates_rejected": conflicting_duplicates_rejected,
            "duplicate_deliveries_idempotent": duplicate_deliveries,
            "invalid_payloads_rejected": invalid_rejections,
            "spans_submitted": TRACE_COUNT * 6,
            "traces_submitted": TRACE_COUNT,
        },
        "overview": overview,
        "sampling": {
            "anomalous_traces": anomalous,
            "error_traces": error_traces,
            "head_10_percent_anomalies_kept": head_anomalies_kept,
            "tail_anomalies_kept": tail_anomalies_kept,
            "tail_anomaly_retention_percent": round(
                tail_anomalies_kept * 100 / anomalous, 2
            ),
        },
        "success": success,
    }


def run_demo(database: str | Path | None = None) -> dict[str, Any]:
    if database is not None:
        path = Path(database).expanduser().absolute()
        if path.exists():
            raise ValidationError("demo database must not already exist")
        return _run(path)
    with tempfile.TemporaryDirectory(prefix="tracelens-demo-") as temporary:
        return _run(Path(temporary) / "traces.sqlite3")
