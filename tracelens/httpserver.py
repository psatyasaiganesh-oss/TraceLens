"""Bounded loopback-first JSON API for TraceLens."""

from __future__ import annotations

import hmac
import json
import math
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .errors import ConflictError, NotFoundError, StorageError, TraceLensError, ValidationError
from .model import Span
from .store import TraceStore

DEFAULT_MAX_BODY_BYTES = 64 * 1024
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(self, *args: Any, max_workers: int = 32, **kwargs: Any):
        self._workers = threading.BoundedSemaphore(max_workers)
        super().__init__(*args, **kwargs)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._workers.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._workers.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._workers.release()


def create_server(
    store: TraceStore,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    bearer_token: str | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> BoundedThreadingHTTPServer:
    if not isinstance(store, TraceStore):
        raise ValidationError("store must be a TraceStore")
    if not isinstance(host, str) or not host:
        raise ValidationError("host must be a non-empty string")
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise ValidationError("port is outside the valid range")
    if not isinstance(max_body_bytes, int) or not 1024 <= max_body_bytes <= 10 * 1024 * 1024:
        raise ValidationError("max_body_bytes is outside the supported range")
    if bearer_token is not None and (
        not isinstance(bearer_token, str) or len(bearer_token) < 16 or len(bearer_token) > 512
    ):
        raise ValidationError("bearer token must contain 16 to 512 characters")
    if host not in LOOPBACK_HOSTS and bearer_token is None:
        raise ValidationError("a bearer token is required for non-loopback binding")

    class Handler(BaseHTTPRequestHandler):
        server_version = "TraceLens/1.0"
        sys_version = ""

        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            try:
                path = urlparse(self.path).path
                if path == "/health":
                    self._send(HTTPStatus.OK, {"status": "ok"})
                    return
                self._authorize()
                if path == "/v1/overview":
                    self._send(HTTPStatus.OK, store.overview())
                    return
                if path == "/v1/services":
                    self._send(HTTPStatus.OK, store.service_graph())
                    return
                prefix = "/v1/traces/"
                if path.startswith(prefix) and "/" not in path[len(prefix) :]:
                    self._send(HTTPStatus.OK, store.get_trace(path[len(prefix) :]))
                    return
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            except Exception as exc:
                self._send_error(exc)

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            try:
                self._authorize()
                path = urlparse(self.path).path
                payload = self._read_json()
                if path == "/v1/spans":
                    if not isinstance(payload, dict) or set(payload) != {"spans"}:
                        raise ValidationError("request must contain only a spans array")
                    if not isinstance(payload["spans"], list):
                        raise ValidationError("spans must be an array")
                    spans = [Span.from_dict(item) for item in payload["spans"]]
                    self._send(HTTPStatus.ACCEPTED, store.ingest_many(spans))
                    return
                prefix = "/v1/traces/"
                suffix = "/finalize"
                if path.startswith(prefix) and path.endswith(suffix):
                    trace_id = path[len(prefix) : -len(suffix)]
                    if "/" in trace_id or not trace_id:
                        raise ValidationError("invalid finalize path")
                    if not isinstance(payload, dict) or not set(payload).issubset(
                        {"normal_sample_rate", "slow_threshold_ms"}
                    ):
                        raise ValidationError("finalize request contains unknown fields")
                    threshold = payload.get("slow_threshold_ms", 100.0)
                    if (
                        isinstance(threshold, bool)
                        or not isinstance(threshold, (int, float))
                        or not math.isfinite(float(threshold))
                        or not 0 <= float(threshold) <= 86_400_000
                    ):
                        raise ValidationError("slow_threshold_ms is invalid")
                    report = store.finalize(
                        trace_id,
                        slow_threshold_ns=int(float(threshold) * 1_000_000),
                        normal_sample_rate=payload.get("normal_sample_rate", 0.1),
                    )
                    self._send(HTTPStatus.OK, report)
                    return
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            except Exception as exc:
                self._send_error(exc)

        def _authorize(self) -> None:
            if bearer_token is None:
                return
            supplied = self.headers.get("Authorization", "")
            expected = f"Bearer {bearer_token}"
            if not hmac.compare_digest(supplied.encode(), expected.encode()):
                raise PermissionError("authentication required")

        def _read_json(self) -> Any:
            if self.headers.get("Transfer-Encoding"):
                raise ValidationError("transfer encoding is not supported")
            content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip()
            if content_type != "application/json":
                raise ValidationError("Content-Type must be application/json")
            raw_length = self.headers.get("Content-Length")
            if raw_length is None or not raw_length.isdigit():
                raise ValidationError("a decimal Content-Length is required")
            length = int(raw_length)
            if length > max_body_bytes:
                raise BodyTooLargeError("request body exceeds the configured limit")
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValidationError("request body ended early")
            try:
                return json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError("request body is not valid UTF-8 JSON") from exc

        def _send_error(self, exc: Exception) -> None:
            if isinstance(exc, BodyTooLargeError):
                status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
            elif isinstance(exc, PermissionError):
                status = HTTPStatus.UNAUTHORIZED
            elif isinstance(exc, ConflictError):
                status = HTTPStatus.CONFLICT
            elif isinstance(exc, NotFoundError):
                status = HTTPStatus.NOT_FOUND
            elif isinstance(exc, StorageError):
                status = HTTPStatus.SERVICE_UNAVAILABLE
            elif isinstance(exc, (ValidationError, ValueError)):
                status = HTTPStatus.BAD_REQUEST
            else:
                status = HTTPStatus.INTERNAL_SERVER_ERROR
            body = {"error": exc.__class__.__name__, "message": str(exc)}
            if status == HTTPStatus.INTERNAL_SERVER_ERROR:
                body = {"error": "internal_error", "message": "internal server error"}
            self._send(status, body)

        def _send(self, status: HTTPStatus, payload: Any) -> None:
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

    return BoundedThreadingHTTPServer((host, port), Handler)


class BodyTooLargeError(TraceLensError):
    pass
