from __future__ import annotations

import unittest

from tracelens.errors import ValidationError
from tracelens.model import TraceContext
from tracelens.sdk import Tracer

from .helpers import ROOT, TRACE_A


class Sequence:
    def __init__(self, *values: int):
        self.values = iter(values)

    def __call__(self) -> int:
        return next(self.values)


class IdSequence:
    def __init__(self, *values: str):
        self.values = iter(values)

    def __call__(self, byte_count: int) -> str:
        value = next(self.values)
        if len(value) != byte_count * 2:
            raise AssertionError("test ID has the wrong length")
        return value


class TracerTests(unittest.TestCase):
    def test_root_span_is_exported_with_monotonic_duration(self) -> None:
        exported = []
        tracer = Tracer(
            "checkout",
            exported.append,
            wall_clock_ns=Sequence(1_000),
            monotonic_ns=Sequence(100, 175),
            id_generator=IdSequence("a" * 16, TRACE_A),
        )
        with tracer.span("POST /checkout", kind="SERVER") as scope:
            scope.set_attribute("http.status_code", 200)
            self.assertEqual(scope.traceparent(), f"00-{TRACE_A}-{'a' * 16}-01")
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0].duration_ns, 75)
        self.assertEqual(exported[0].start_ns, 1_000)
        self.assertIsNone(exported[0].parent_span_id)
        self.assertEqual(exported[0].attributes["http.status_code"], 200)

    def test_nested_span_inherits_trace_and_parent(self) -> None:
        exported = []
        tracer = Tracer(
            "checkout",
            exported.append,
            wall_clock_ns=Sequence(1_000, 1_010),
            monotonic_ns=Sequence(100, 110, 130, 160),
            id_generator=IdSequence("a" * 16, TRACE_A, "b" * 16),
        )
        with tracer.span("root"):
            with tracer.span("child"):
                pass
        child, root = exported
        self.assertEqual(child.trace_id, TRACE_A)
        self.assertEqual(child.parent_span_id, "a" * 16)
        self.assertEqual(root.parent_span_id, None)
        self.assertEqual((child.duration_ns, root.duration_ns), (20, 60))

    def test_incoming_traceparent_is_propagated(self) -> None:
        exported = []
        incoming = TraceContext(TRACE_A, ROOT, False).format()
        tracer = Tracer(
            "inventory",
            exported.append,
            wall_clock_ns=Sequence(2_000),
            monotonic_ns=Sequence(20, 25),
            id_generator=IdSequence("c" * 16),
        )
        with tracer.span("reserve", incoming=incoming) as scope:
            self.assertTrue(scope.traceparent().endswith("-00"))
        self.assertEqual(exported[0].trace_id, TRACE_A)
        self.assertEqual(exported[0].parent_span_id, ROOT)

    def test_exception_marks_span_without_suppressing_it(self) -> None:
        exported = []
        tracer = Tracer(
            "payments",
            exported.append,
            wall_clock_ns=Sequence(1),
            monotonic_ns=Sequence(1, 2),
            id_generator=IdSequence("a" * 16, TRACE_A),
        )
        with self.assertRaisesRegex(RuntimeError, "declined"):
            with tracer.span("authorize"):
                raise RuntimeError("declined")
        self.assertEqual(exported[0].status, "ERROR")
        self.assertEqual(exported[0].attributes["exception.type"], "RuntimeError")

    def test_exporter_failure_does_not_mask_application_failure(self) -> None:
        def fail_export(_span: object) -> None:
            raise OSError("collector unavailable")

        tracer = Tracer(
            "payments",
            fail_export,
            wall_clock_ns=Sequence(1),
            monotonic_ns=Sequence(1, 2),
            id_generator=IdSequence("a" * 16, TRACE_A),
        )
        with self.assertRaisesRegex(RuntimeError, "application"):
            with tracer.span("authorize"):
                raise RuntimeError("application")

    def test_exporter_failure_is_visible_without_application_failure(self) -> None:
        def fail_export(_span: object) -> None:
            raise OSError("collector unavailable")

        tracer = Tracer(
            "payments",
            fail_export,
            wall_clock_ns=Sequence(1),
            monotonic_ns=Sequence(1, 2),
            id_generator=IdSequence("a" * 16, TRACE_A),
        )
        with self.assertRaisesRegex(OSError, "collector unavailable"):
            with tracer.span("authorize"):
                pass

    def test_scope_methods_require_entry(self) -> None:
        tracer = Tracer("api", lambda _span: None)
        scope = tracer.span("work")
        with self.assertRaises(ValidationError):
            scope.traceparent()
        with self.assertRaises(ValidationError):
            scope.set_attribute("key", "value")

    def test_invalid_configuration_and_incoming_context_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            Tracer("bad\nservice", lambda _span: None)
        with self.assertRaises(ValidationError):
            Tracer("api", object())
        tracer = Tracer("api", lambda _span: None)
        with self.assertRaises(ValidationError):
            tracer.span("work", incoming="not-a-traceparent")


if __name__ == "__main__":
    unittest.main()
