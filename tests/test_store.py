from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from tracelens.errors import ConflictError, NotFoundError, StorageError, ValidationError
from tracelens.model import Span
from tracelens.store import TraceStore, deterministic_sample

from .helpers import TRACE_A, TRACE_B, basic_trace, child_span, make_store, root_span


class StoreFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = make_store(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()


class InitializationTests(unittest.TestCase):
    def test_initializes_and_reopens_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state" / "traces.sqlite3"
            first = TraceStore(path)
            first.ingest(root_span())
            second = TraceStore(path)
            self.assertEqual(second.get_trace(TRACE_A)["stored_span_count"], 1)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_capacity_configuration_is_validated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            for value in (0, True, 1_000_001):
                with self.subTest(value=value), self.assertRaises(ValidationError):
                    TraceStore(Path(temporary) / f"{value}.db", max_active_traces=value)

    def test_database_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "target"
            target.write_bytes(b"")
            link = root / "link"
            os.symlink(target, link)
            with self.assertRaises(ValidationError):
                TraceStore(link)


class IngestionTests(StoreFixture):
    def test_ingests_out_of_order_batch(self) -> None:
        result = self.store.ingest_many([child_span(), root_span()], received_ns=10)
        self.assertEqual(result["accepted"], 2)
        self.assertEqual(self.store.get_trace(TRACE_A)["state"], "active")

    def test_identical_delivery_is_idempotent(self) -> None:
        first = self.store.ingest(root_span())
        second = self.store.ingest(root_span())
        self.assertEqual(first["accepted"], 1)
        self.assertEqual(second["duplicates"], 1)
        self.assertEqual(self.store.get_trace(TRACE_A)["stored_span_count"], 1)

    def test_conflicting_duplicate_is_rejected(self) -> None:
        self.store.ingest(root_span())
        changed = root_span(duration_ns=51_000_000)
        with self.assertRaises(ConflictError):
            self.store.ingest(changed)

    def test_batch_conflict_rolls_back_every_insert(self) -> None:
        self.store.ingest(root_span())
        new_trace = root_span(trace_id=TRACE_B)
        conflict = root_span(duration_ns=51_000_000)
        with self.assertRaises(ConflictError):
            self.store.ingest_many([new_trace, conflict])
        with self.assertRaises(NotFoundError):
            self.store.get_trace(TRACE_B)

    def test_active_trace_limit_is_enforced(self) -> None:
        store = TraceStore(self.root / "limited.db", max_active_traces=1)
        store.ingest(root_span())
        with self.assertRaises(StorageError):
            store.ingest(root_span(trace_id=TRACE_B))

    def test_span_limit_is_enforced(self) -> None:
        store = TraceStore(self.root / "limited-spans.db", max_spans_per_trace=1)
        store.ingest(root_span())
        with self.assertRaises(StorageError):
            store.ingest(child_span())

    def test_batch_size_is_bounded(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.ingest_many([])
        spans = [root_span(trace_id=f"{index + 1:032x}") for index in range(101)]
        with self.assertRaises(ValidationError):
            self.store.ingest_many(spans)

    def test_received_timestamp_is_validated(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.ingest(root_span(), received_ns=-1)


class FinalizationTests(StoreFixture):
    def _ingest(self, trace_id: str = TRACE_A, **root_values) -> None:
        spans = basic_trace(trace_id=trace_id, **root_values)
        self.store.ingest_many(spans)

    def test_slow_trace_is_always_kept(self) -> None:
        self._ingest(duration_ns=200_000_000)
        report = self.store.finalize(
            TRACE_A, slow_threshold_ns=100_000_000, normal_sample_rate=0
        )
        self.assertTrue(report["kept"])
        self.assertEqual(report["sampling_reason"], "slow")

    def test_error_trace_is_always_kept(self) -> None:
        self._ingest(status="ERROR")
        report = self.store.finalize(TRACE_A, normal_sample_rate=0)
        self.assertTrue(report["kept"])
        self.assertEqual(report["sampling_reason"], "error")

    def test_normal_trace_can_be_dropped_and_spans_removed(self) -> None:
        self._ingest()
        report = self.store.finalize(TRACE_A, normal_sample_rate=0)
        fetched = self.store.get_trace(TRACE_A)
        self.assertFalse(report["kept"])
        self.assertEqual(fetched["state"], "dropped")
        self.assertEqual(fetched["stored_span_count"], 0)

    def test_normal_trace_can_be_probabilistically_kept(self) -> None:
        self._ingest()
        report = self.store.finalize(TRACE_A, normal_sample_rate=1)
        self.assertTrue(report["kept"])
        self.assertEqual(report["sampling_reason"], "probabilistic")
        self.assertEqual(self.store.get_trace(TRACE_A)["stored_span_count"], 2)

    def test_finalize_is_idempotent(self) -> None:
        self._ingest()
        first = self.store.finalize(TRACE_A, normal_sample_rate=1, finalized_ns=20)
        second = self.store.finalize(TRACE_A, normal_sample_rate=0, finalized_ns=30)
        self.assertFalse(first["already_finalized"])
        self.assertTrue(second["already_finalized"])
        self.assertEqual(second["sampling_reason"], first["sampling_reason"])

    def test_orphan_trace_cannot_finalize(self) -> None:
        self.store.ingest(child_span())
        with self.assertRaises(ValidationError):
            self.store.finalize(TRACE_A)
        self.assertEqual(self.store.get_trace(TRACE_A)["state"], "active")

    def test_ingestion_after_finalize_is_rejected(self) -> None:
        self._ingest()
        self.store.finalize(TRACE_A, normal_sample_rate=1)
        with self.assertRaises(ConflictError):
            self.store.ingest(
                child_span(span_id="c" * 16, start_ns=1_030_000_000, duration_ns=1_000_000)
            )

    def test_unknown_trace_is_rejected(self) -> None:
        with self.assertRaises(NotFoundError):
            self.store.finalize(TRACE_A)

    def test_sampling_inputs_are_validated(self) -> None:
        self._ingest()
        for rate in (-0.1, 1.1, float("nan"), True):
            with self.subTest(rate=rate), self.assertRaises(ValidationError):
                self.store.finalize(TRACE_A, normal_sample_rate=rate)
        with self.assertRaises(ValidationError):
            self.store.finalize(TRACE_A, slow_threshold_ns=-1)


class AggregateTests(StoreFixture):
    def test_overview_combines_kept_and_dropped_summaries(self) -> None:
        self.store.ingest_many(basic_trace(trace_id=TRACE_A))
        self.store.finalize(TRACE_A, normal_sample_rate=0)
        self.store.ingest_many(basic_trace(trace_id=TRACE_B, duration_ns=200_000_000))
        self.store.finalize(TRACE_B, normal_sample_rate=0)
        overview = self.store.overview()
        self.assertEqual(overview["finalized_traces"], 2)
        self.assertEqual(overview["kept_traces"], 1)
        self.assertEqual(overview["dropped_traces"], 1)
        self.assertEqual(overview["span_storage_reduction_percent"], 50.0)

    def test_service_graph_aggregates_kept_traces(self) -> None:
        self.store.ingest_many(basic_trace())
        self.store.finalize(TRACE_A, normal_sample_rate=1)
        graph = self.store.service_graph()
        self.assertEqual(graph["services"], ["catalog", "gateway"])
        self.assertEqual(graph["edges"][0]["calls"], 1)

    def test_purge_removes_only_stale_active_traces(self) -> None:
        self.store.ingest(root_span(), received_ns=10)
        self.store.ingest(root_span(trace_id=TRACE_B), received_ns=30)
        result = self.store.purge_stale(before_received_ns=20)
        self.assertEqual(result, {"purged_spans": 1, "purged_traces": 1})
        with self.assertRaises(NotFoundError):
            self.store.get_trace(TRACE_A)
        self.assertEqual(self.store.get_trace(TRACE_B)["state"], "active")

    def test_deterministic_sampling_is_stable(self) -> None:
        first = deterministic_sample(TRACE_A, 0.5)
        self.assertEqual(first, deterministic_sample(TRACE_A, 0.5))
        self.assertFalse(deterministic_sample(TRACE_A, 0))
        self.assertTrue(deterministic_sample(TRACE_A, 1))
