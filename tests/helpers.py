from __future__ import annotations

from pathlib import Path

from tracelens.model import Span
from tracelens.store import TraceStore

TRACE_A = "1" * 32
TRACE_B = "2" * 32
ROOT = "a" * 16
CHILD = "b" * 16


def root_span(
    *,
    trace_id: str = TRACE_A,
    duration_ns: int = 50_000_000,
    status: str = "OK",
) -> Span:
    return Span(
        trace_id=trace_id,
        span_id=ROOT,
        parent_span_id=None,
        service="gateway",
        name="GET /items",
        kind="SERVER",
        start_ns=1_000_000_000,
        duration_ns=duration_ns,
        status=status,
        attributes={"http.route": "/items"},
    )


def child_span(
    *,
    trace_id: str = TRACE_A,
    span_id: str = CHILD,
    parent_span_id: str = ROOT,
    service: str = "catalog",
    start_ns: int = 1_005_000_000,
    duration_ns: int = 20_000_000,
    status: str = "OK",
) -> Span:
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        service=service,
        name="lookup",
        kind="CLIENT",
        start_ns=start_ns,
        duration_ns=duration_ns,
        status=status,
        attributes={"cache.hit": False},
    )


def basic_trace(**root_values):
    root = root_span(**root_values)
    child = child_span(trace_id=root.trace_id)
    return [root, child]


def make_store(root: Path, **kwargs) -> TraceStore:
    return TraceStore(root / "traces.sqlite3", **kwargs)
