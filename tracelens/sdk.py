"""Tiny context-manager instrumentation SDK with trace-context propagation."""

from __future__ import annotations

import contextvars
import secrets
import time
from collections.abc import Callable
from types import TracebackType
from typing import Any

from .errors import ValidationError
from .model import Span, TraceContext
from .store import TraceStore

_current_context: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "tracelens_current_context", default=None
)


class StoreExporter:
    """Export completed spans directly into a TraceStore."""

    def __init__(self, store: TraceStore):
        self.store = store

    def __call__(self, span: Span) -> None:
        self.store.ingest(span)


class Tracer:
    """Create nested spans without third-party instrumentation dependencies."""

    def __init__(
        self,
        service: str,
        exporter: Callable[[Span], Any],
        *,
        wall_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        id_generator: Callable[[int], str] = secrets.token_hex,
    ):
        # Reuse Span validation for service naming without emitting anything.
        probe = Span(
            trace_id="1" * 32,
            span_id="1" * 16,
            parent_span_id=None,
            service=service,
            name="validation",
            kind="INTERNAL",
            start_ns=0,
            duration_ns=0,
        )
        if not callable(exporter):
            raise ValidationError("exporter must be callable")
        self.service = probe.service
        self.exporter = exporter
        self.wall_clock_ns = wall_clock_ns
        self.monotonic_ns = monotonic_ns
        self.id_generator = id_generator

    def span(
        self,
        name: str,
        *,
        kind: str = "INTERNAL",
        incoming: TraceContext | str | None = None,
        attributes: dict[str, str | int | float | bool] | None = None,
    ) -> "SpanScope":
        if isinstance(incoming, str):
            incoming = TraceContext.parse(incoming)
        if incoming is not None and not isinstance(incoming, TraceContext):
            raise ValidationError("incoming context must be TraceContext, traceparent, or None")
        return SpanScope(
            tracer=self,
            name=name,
            kind=kind,
            incoming=incoming,
            attributes=attributes or {},
        )


class SpanScope:
    def __init__(
        self,
        *,
        tracer: Tracer,
        name: str,
        kind: str,
        incoming: TraceContext | None,
        attributes: dict[str, str | int | float | bool],
    ):
        self.tracer = tracer
        self.name = name
        self.kind = kind
        self.incoming = incoming
        self.attributes = dict(attributes)
        self.context: TraceContext | None = None
        self._token: contextvars.Token[TraceContext | None] | None = None
        self._start_wall_ns: int | None = None
        self._start_mono_ns: int | None = None

    def __enter__(self) -> "SpanScope":
        parent = self.incoming or _current_context.get()
        span_id = self.tracer.id_generator(8)
        if parent is None:
            trace_id = self.tracer.id_generator(16)
            parent_span_id = None
            sampled = True
        else:
            trace_id = parent.trace_id
            parent_span_id = parent.parent_span_id
            sampled = parent.sampled
        self.context = TraceContext(trace_id, span_id, sampled)
        self._start_wall_ns = self.tracer.wall_clock_ns()
        self._start_mono_ns = self.tracer.monotonic_ns()
        self._token = _current_context.set(self.context)
        return self

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        if self.context is None:
            raise ValidationError("span scope has not started")
        self.attributes[key] = value

    def traceparent(self) -> str:
        if self.context is None:
            raise ValidationError("span scope has not started")
        return self.context.format()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if (
            self.context is None
            or self._token is None
            or self._start_wall_ns is None
            or self._start_mono_ns is None
        ):
            raise ValidationError("span scope was not entered")
        end_mono_ns = self.tracer.monotonic_ns()
        _current_context.reset(self._token)
        attributes = dict(self.attributes)
        status = "OK"
        if exception_type is not None:
            status = "ERROR"
            attributes["exception.type"] = exception_type.__name__[:256]
        span = Span(
            trace_id=self.context.trace_id,
            span_id=self.context.parent_span_id,
            parent_span_id=(self.incoming or _current_context.get()).parent_span_id
            if (self.incoming or _current_context.get()) is not None
            else None,
            service=self.tracer.service,
            name=self.name,
            kind=self.kind,
            start_ns=self._start_wall_ns,
            duration_ns=max(0, end_mono_ns - self._start_mono_ns),
            status=status,
            attributes=attributes,
        )
        try:
            self.tracer.exporter(span)
        except Exception:
            if exception_type is None:
                raise
            # Instrumentation must not replace an application exception.
        return False
