from __future__ import annotations

import math
import unittest

from tracelens.analysis import analyze_trace, percentile
from tracelens.errors import ValidationError
from tracelens.model import Span, TraceContext

from .helpers import CHILD, ROOT, TRACE_A, TRACE_B, child_span, root_span


class SpanModelTests(unittest.TestCase):
    def test_span_round_trip(self) -> None:
        span = root_span()
        self.assertEqual(Span.from_dict(span.to_dict()), span)

    def test_span_schema_is_strict(self) -> None:
        payload = root_span().to_dict()
        payload["extra"] = True
        with self.assertRaises(ValidationError):
            Span.from_dict(payload)
        payload.pop("extra")
        payload.pop("name")
        with self.assertRaises(ValidationError):
            Span.from_dict(payload)

    def test_zero_and_noncanonical_ids_are_rejected(self) -> None:
        for value in ("0" * 32, "A" * 32, "x" * 32, "1" * 31):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                root_span(trace_id=value)

    def test_self_parent_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            Span(
                trace_id=TRACE_A,
                span_id=ROOT,
                parent_span_id=ROOT,
                service="api",
                name="self",
                kind="INTERNAL",
                start_ns=1,
                duration_ns=1,
            )

    def test_unsafe_names_are_rejected(self) -> None:
        payload = root_span().to_dict()
        for field, value in (("service", "bad\nservice"), ("name", "")):
            changed = dict(payload, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValidationError):
                Span.from_dict(changed)

    def test_kind_and_status_are_enums(self) -> None:
        payload = root_span().to_dict()
        with self.assertRaises(ValidationError):
            Span.from_dict(dict(payload, kind="DATABASE"))
        with self.assertRaises(ValidationError):
            Span.from_dict(dict(payload, status="UNKNOWN"))

    def test_timestamp_bounds_are_enforced(self) -> None:
        payload = root_span().to_dict()
        for field, value in (("start_ns", -1), ("duration_ns", -1), ("duration_ns", 10**20)):
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                Span.from_dict(dict(payload, **{field: value}))

    def test_supported_attribute_types_round_trip(self) -> None:
        payload = root_span().to_dict()
        payload["attributes"] = {"a": "x", "b": 2, "c": 1.5, "d": True}
        self.assertEqual(Span.from_dict(payload).attributes, payload["attributes"])

    def test_attribute_key_and_values_are_bounded(self) -> None:
        payload = root_span().to_dict()
        bad_values = (
            {"bad key": "x"},
            {"good": [1, 2]},
            {"good": "x" * 257},
            {"good": math.inf},
        )
        for attributes in bad_values:
            with self.subTest(attributes=attributes), self.assertRaises(ValidationError):
                Span.from_dict(dict(payload, attributes=attributes))


class TraceContextTests(unittest.TestCase):
    def test_traceparent_round_trip(self) -> None:
        context = TraceContext(TRACE_A, ROOT, True)
        self.assertEqual(TraceContext.parse(context.format()), context)

    def test_unsampled_flag_round_trip(self) -> None:
        context = TraceContext(TRACE_A, ROOT, False)
        self.assertTrue(context.format().endswith("-00"))
        self.assertFalse(TraceContext.parse(context.format()).sampled)

    def test_invalid_traceparent_is_rejected(self) -> None:
        values = (
            f"01-{TRACE_A}-{ROOT}-01",
            f"00-{'A' * 32}-{ROOT}-01",
            f"00-{'0' * 32}-{ROOT}-01",
            "short",
        )
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                TraceContext.parse(value)

    def test_new_context_has_canonical_lengths(self) -> None:
        first = TraceContext.new()
        second = TraceContext.new()
        self.assertEqual(len(first.trace_id), 32)
        self.assertEqual(len(first.parent_span_id), 16)
        self.assertNotEqual(first, second)


class AnalysisTests(unittest.TestCase):
    def test_simple_trace_analysis(self) -> None:
        report = analyze_trace([root_span(duration_ns=25_000_000), child_span()])
        self.assertEqual(report["trace_id"], TRACE_A)
        self.assertEqual(report["span_count"], 2)
        self.assertEqual(report["dependency_edges"][0]["target"], "catalog")
        self.assertEqual(report["bottleneck_service"], "catalog")

    def test_overlapping_children_use_interval_union_for_self_time(self) -> None:
        child_two = child_span(
            span_id="c" * 16,
            service="search",
            start_ns=1_015_000_000,
            duration_ns=20_000_000,
        )
        report = analyze_trace([root_span(), child_span(), child_two])
        self.assertEqual(report["self_time_ns"]["gateway"], 20_000_000)

    def test_orphan_span_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "orphan"):
            analyze_trace([root_span(), child_span(parent_span_id="c" * 16)])

    def test_multiple_roots_are_rejected(self) -> None:
        second = Span.from_dict(dict(root_span().to_dict(), span_id="c" * 16))
        with self.assertRaisesRegex(ValidationError, "exactly one root"):
            analyze_trace([root_span(), second])

    def test_duplicate_span_ids_are_rejected(self) -> None:
        duplicate = Span.from_dict(dict(root_span().to_dict(), name="duplicate"))
        with self.assertRaisesRegex(ValidationError, "unique"):
            analyze_trace([root_span(), duplicate])

    def test_cycle_is_rejected(self) -> None:
        a = child_span(span_id="c" * 16, parent_span_id="d" * 16)
        b = child_span(span_id="d" * 16, parent_span_id="c" * 16)
        with self.assertRaisesRegex(ValidationError, "cycle"):
            analyze_trace([root_span(), a, b])

    def test_mixed_trace_ids_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "same trace_id"):
            analyze_trace([root_span(), child_span(trace_id=TRACE_B)])

    def test_clock_skew_within_tolerance_is_clipped(self) -> None:
        skewed = child_span(start_ns=999_000_000, duration_ns=10_000_000)
        report = analyze_trace([root_span(), skewed])
        self.assertEqual(report["span_count"], 2)

    def test_clock_skew_beyond_tolerance_is_rejected(self) -> None:
        skewed = child_span(start_ns=990_000_000, duration_ns=10_000_000)
        with self.assertRaisesRegex(ValidationError, "before"):
            analyze_trace([root_span(), skewed])

    def test_error_count_is_reported(self) -> None:
        report = analyze_trace([root_span(status="ERROR"), child_span(status="ERROR")])
        self.assertEqual(report["error_count"], 2)

    def test_percentile_nearest_rank(self) -> None:
        self.assertEqual(percentile([1, 2, 3, 4], 0.50), 2.0)
        self.assertEqual(percentile([1, 2, 3, 4], 0.99), 4.0)

    def test_percentile_empty_and_invalid(self) -> None:
        self.assertIsNone(percentile([], 0.95))
        for quantile in (1.1, -0.1, True, math.nan):
            with self.subTest(quantile=quantile), self.assertRaises(ValidationError):
                percentile([1], quantile)

    def test_clock_skew_tolerance_rejects_boolean(self) -> None:
        with self.assertRaises(ValidationError):
            analyze_trace([root_span()], clock_skew_tolerance_ns=True)
