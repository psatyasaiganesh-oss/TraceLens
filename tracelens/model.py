"""Validated span model and W3C-style trace context helpers."""

from __future__ import annotations

import json
import math
import re
import secrets
from dataclasses import dataclass, field
from typing import Any, Mapping

from .errors import ValidationError

TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/ -]*$")
ATTRIBUTE_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
TRACEPARENT_RE = re.compile(
    r"^00-([0-9a-f]{32})-([0-9a-f]{16})-(00|01)$"
)
KINDS = frozenset({"SERVER", "CLIENT", "INTERNAL", "PRODUCER", "CONSUMER"})
STATUSES = frozenset({"OK", "ERROR"})
SPAN_FIELDS = frozenset(
    {
        "trace_id",
        "span_id",
        "parent_span_id",
        "service",
        "name",
        "kind",
        "start_ns",
        "duration_ns",
        "status",
        "attributes",
    }
)


def _identifier(value: Any, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value) or set(value) == {"0"}:
        raise ValidationError(f"{label} must be canonical non-zero lowercase hexadecimal")
    return value


def validate_trace_id(value: Any) -> str:
    return _identifier(value, TRACE_ID_RE, "trace_id")


def validate_span_id(value: Any, label: str = "span_id") -> str:
    return _identifier(value, SPAN_ID_RE, label)


def _name(value: Any, label: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or not NAME_RE.fullmatch(value)
    ):
        raise ValidationError(f"{label} must contain 1 to {maximum} safe characters")
    return value


def _validate_attributes(value: Any) -> dict[str, str | int | float | bool]:
    if not isinstance(value, Mapping) or len(value) > 24:
        raise ValidationError("attributes must be an object with at most 24 entries")
    clean: dict[str, str | int | float | bool] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not ATTRIBUTE_KEY_RE.fullmatch(key):
            raise ValidationError(f"invalid attribute key: {key!r}")
        if isinstance(item, bool):
            clean[key] = item
        elif isinstance(item, int) and not isinstance(item, bool):
            if not -(2**63) <= item <= 2**63 - 1:
                raise ValidationError(f"integer attribute is out of range: {key}")
            clean[key] = item
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ValidationError(f"float attribute must be finite: {key}")
            clean[key] = item
        elif isinstance(item, str) and len(item) <= 256:
            clean[key] = item
        else:
            raise ValidationError(f"unsupported or oversized attribute value: {key}")
    encoded = json.dumps(clean, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > 4096:
        raise ValidationError("encoded attributes exceed 4096 bytes")
    return clean


@dataclass(frozen=True, slots=True)
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    service: str
    name: str
    kind: str
    start_ns: int
    duration_ns: int
    status: str = "OK"
    attributes: dict[str, str | int | float | bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "trace_id", validate_trace_id(self.trace_id))
        object.__setattr__(self, "span_id", validate_span_id(self.span_id))
        if self.parent_span_id is not None:
            parent = validate_span_id(self.parent_span_id, "parent_span_id")
            if parent == self.span_id:
                raise ValidationError("a span cannot be its own parent")
            object.__setattr__(self, "parent_span_id", parent)
        object.__setattr__(self, "service", _name(self.service, "service", 80))
        object.__setattr__(self, "name", _name(self.name, "name", 120))
        if self.kind not in KINDS:
            raise ValidationError(f"kind must be one of {sorted(KINDS)}")
        if self.status not in STATUSES:
            raise ValidationError(f"status must be one of {sorted(STATUSES)}")
        if not isinstance(self.start_ns, int) or isinstance(self.start_ns, bool):
            raise ValidationError("start_ns must be an integer")
        if not 0 <= self.start_ns <= 2**63 - 1:
            raise ValidationError("start_ns is outside the supported range")
        if not isinstance(self.duration_ns, int) or isinstance(self.duration_ns, bool):
            raise ValidationError("duration_ns must be an integer")
        if not 0 <= self.duration_ns <= 24 * 60 * 60 * 1_000_000_000:
            raise ValidationError("duration_ns must be between zero and 24 hours")
        if self.start_ns + self.duration_ns > 2**63 - 1:
            raise ValidationError("span end timestamp overflows the supported range")
        object.__setattr__(self, "attributes", _validate_attributes(self.attributes))

    @property
    def end_ns(self) -> int:
        return self.start_ns + self.duration_ns

    @classmethod
    def from_dict(cls, value: Any) -> "Span":
        if not isinstance(value, dict) or set(value) != SPAN_FIELDS:
            raise ValidationError("span fields do not match the schema")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "attributes": dict(self.attributes),
            "duration_ns": self.duration_ns,
            "kind": self.kind,
            "name": self.name,
            "parent_span_id": self.parent_span_id,
            "service": self.service,
            "span_id": self.span_id,
            "start_ns": self.start_ns,
            "status": self.status,
            "trace_id": self.trace_id,
        }


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    parent_span_id: str
    sampled: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "trace_id", validate_trace_id(self.trace_id))
        object.__setattr__(
            self,
            "parent_span_id",
            validate_span_id(self.parent_span_id, "parent_span_id"),
        )
        if not isinstance(self.sampled, bool):
            raise ValidationError("sampled must be boolean")

    @classmethod
    def new(cls, *, sampled: bool = True) -> "TraceContext":
        return cls(secrets.token_hex(16), secrets.token_hex(8), sampled)

    @classmethod
    def parse(cls, header: str) -> "TraceContext":
        if not isinstance(header, str) or len(header) != 55:
            raise ValidationError("traceparent must be a 55-character string")
        match = TRACEPARENT_RE.fullmatch(header)
        if match is None:
            raise ValidationError("traceparent is not canonical version 00")
        trace_id, parent_span_id, flags = match.groups()
        return cls(trace_id, parent_span_id, flags == "01")

    def format(self) -> str:
        return f"00-{self.trace_id}-{self.parent_span_id}-{'01' if self.sampled else '00'}"
