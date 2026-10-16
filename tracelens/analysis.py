"""Trace-graph validation, latency attribution, and aggregate statistics."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

from .errors import ValidationError
from .model import Span

DEFAULT_CLOCK_SKEW_TOLERANCE_NS = 5_000_000


def percentile(values: Iterable[int | float], quantile: float) -> float | None:
    """Return an exact nearest-rank percentile with no interpolation."""

    if (
        isinstance(quantile, bool)
        or not isinstance(quantile, (int, float))
        or not math.isfinite(float(quantile))
        or not 0 <= float(quantile) <= 1
    ):
        raise ValidationError("quantile must be between zero and one")
    ordered = sorted(values)
    if not ordered:
        return None
    rank = max(1, math.ceil(quantile * len(ordered)))
    return float(ordered[rank - 1])


def _union_length(intervals: list[tuple[int, int]]) -> int:
    if not intervals:
        return 0
    total = 0
    start, end = sorted(intervals)[0]
    for next_start, next_end in sorted(intervals)[1:]:
        if next_start <= end:
            end = max(end, next_end)
        else:
            total += end - start
            start, end = next_start, next_end
    return total + end - start


def analyze_trace(
    spans: Iterable[Span],
    *,
    clock_skew_tolerance_ns: int = DEFAULT_CLOCK_SKEW_TOLERANCE_NS,
) -> dict[str, Any]:
    """Validate a complete tree and attribute root wall time to active spans."""

    span_list = list(spans)
    if not span_list:
        raise ValidationError("cannot analyze an empty trace")
    if (
        not isinstance(clock_skew_tolerance_ns, int)
        or isinstance(clock_skew_tolerance_ns, bool)
        or clock_skew_tolerance_ns < 0
    ):
        raise ValidationError("clock-skew tolerance must be a non-negative integer")
    trace_ids = {span.trace_id for span in span_list}
    if len(trace_ids) != 1:
        raise ValidationError("all spans must have the same trace_id")
    by_id = {span.span_id: span for span in span_list}
    if len(by_id) != len(span_list):
        raise ValidationError("span ids must be unique within a trace")
    roots = [span for span in span_list if span.parent_span_id is None]
    if len(roots) != 1:
        raise ValidationError("a finalized trace must contain exactly one root span")
    root = roots[0]
    children: dict[str, list[Span]] = defaultdict(list)
    for span in span_list:
        if span.parent_span_id is not None:
            if span.parent_span_id not in by_id:
                raise ValidationError(f"orphan span: {span.span_id}")
            children[span.parent_span_id].append(span)

    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def depth(span: Span) -> int:
        if span.span_id in depths:
            return depths[span.span_id]
        if span.span_id in visiting:
            raise ValidationError("trace graph contains a cycle")
        visiting.add(span.span_id)
        value = 0 if span.parent_span_id is None else depth(by_id[span.parent_span_id]) + 1
        visiting.remove(span.span_id)
        depths[span.span_id] = value
        return value

    for span in span_list:
        depth(span)
        if span.start_ns < root.start_ns - clock_skew_tolerance_ns:
            raise ValidationError(f"span starts before the root beyond tolerance: {span.span_id}")
        if span.end_ns > root.end_ns + clock_skew_tolerance_ns:
            raise ValidationError(f"span ends after the root beyond tolerance: {span.span_id}")
        if span.parent_span_id is not None:
            parent = by_id[span.parent_span_id]
            if span.start_ns < parent.start_ns - clock_skew_tolerance_ns:
                raise ValidationError(f"child starts before parent beyond tolerance: {span.span_id}")
            if span.end_ns > parent.end_ns + clock_skew_tolerance_ns:
                raise ValidationError(f"child ends after parent beyond tolerance: {span.span_id}")

    self_time_by_span: dict[str, int] = {}
    self_time_by_service: dict[str, int] = defaultdict(int)
    dependencies: dict[tuple[str, str], int] = defaultdict(int)
    for span in span_list:
        covered: list[tuple[int, int]] = []
        for child in children.get(span.span_id, []):
            clipped_start = max(span.start_ns, child.start_ns)
            clipped_end = min(span.end_ns, child.end_ns)
            if clipped_end > clipped_start:
                covered.append((clipped_start, clipped_end))
            if child.service != span.service:
                dependencies[(span.service, child.service)] += 1
        own = max(0, span.duration_ns - _union_length(covered))
        self_time_by_span[span.span_id] = own
        self_time_by_service[span.service] += own

    boundaries = {root.start_ns, root.end_ns}
    clipped_intervals: dict[str, tuple[int, int]] = {}
    for span in span_list:
        start = max(root.start_ns, span.start_ns)
        end = min(root.end_ns, span.end_ns)
        clipped_intervals[span.span_id] = (start, end)
        boundaries.update((start, end))
    ordered_boundaries = sorted(boundaries)
    latency_by_span: dict[str, int] = defaultdict(int)
    timeline: list[dict[str, Any]] = []
    for index in range(len(ordered_boundaries) - 1):
        start = ordered_boundaries[index]
        end = ordered_boundaries[index + 1]
        if end <= start:
            continue
        active = [
            span
            for span in span_list
            if clipped_intervals[span.span_id][0] <= start
            and clipped_intervals[span.span_id][1] >= end
        ]
        if not active:
            continue
        selected = max(
            active,
            key=lambda item: (depths[item.span_id], item.end_ns, item.span_id),
        )
        duration = end - start
        latency_by_span[selected.span_id] += duration
        if timeline and timeline[-1]["span_id"] == selected.span_id:
            timeline[-1]["duration_ns"] += duration
            timeline[-1]["end_ns"] = end
        else:
            timeline.append(
                {
                    "duration_ns": duration,
                    "end_ns": end,
                    "name": selected.name,
                    "service": selected.service,
                    "span_id": selected.span_id,
                    "start_ns": start,
                }
            )

    latency_by_service: dict[str, int] = defaultdict(int)
    for span_id, duration in latency_by_span.items():
        latency_by_service[by_id[span_id].service] += duration
    bottleneck_service = max(
        latency_by_service,
        key=lambda service: (latency_by_service[service], service),
    )
    return {
        "bottleneck_service": bottleneck_service,
        "critical_timeline": timeline,
        "dependency_edges": [
            {"calls": calls, "source": source, "target": target}
            for (source, target), calls in sorted(dependencies.items())
        ],
        "error_count": sum(span.status == "ERROR" for span in span_list),
        "latency_attribution_ns": dict(sorted(latency_by_service.items())),
        "root_duration_ns": root.duration_ns,
        "root_service": root.service,
        "self_time_ns": dict(sorted(self_time_by_service.items())),
        "service_count": len({span.service for span in span_list}),
        "services": sorted({span.service for span in span_list}),
        "span_count": len(span_list),
        "trace_id": root.trace_id,
    }
