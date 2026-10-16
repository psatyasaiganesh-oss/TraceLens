"""TraceLens JSON command-line interface."""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence

from . import __version__
from .demo import run_demo
from .errors import TraceLensError, ValidationError
from .httpserver import create_server
from .model import Span
from .store import TraceStore

MAX_INGEST_FILE_BYTES = 2 * 1024 * 1024


def _print(value: Any, *, stream: Any | None = None) -> None:
    print(json.dumps(value, indent=2, sort_keys=True), file=stream or sys.stdout)


def _load_spans(path: str) -> list[Span]:
    source = Path(path).expanduser().absolute()
    if source.is_symlink() or not source.is_file():
        raise ValidationError("span input must be a regular, non-symlink file")
    if source.stat().st_size > MAX_INGEST_FILE_BYTES:
        raise ValidationError("span input file exceeds 2 MiB")
    try:
        payload = json.loads(source.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("span input is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"spans"}:
        raise ValidationError("span input must contain only a spans array")
    if not isinstance(payload["spans"], list):
        raise ValidationError("spans must be an array")
    return [Span.from_dict(item) for item in payload["spans"]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tracelens",
        description="Collect, tail-sample, and diagnose distributed traces.",
    )
    parser.add_argument("--version", action="version", version=f"TraceLens {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser("init", help="initialize a trace database")
    initialize.add_argument("database")

    ingest = commands.add_parser("ingest", help="ingest a JSON span batch")
    ingest.add_argument("database")
    ingest.add_argument("json_file")

    finalize = commands.add_parser("finalize", help="finalize and tail-sample a trace")
    finalize.add_argument("database")
    finalize.add_argument("trace_id")
    finalize.add_argument("--slow-threshold-ms", type=float, default=100.0)
    finalize.add_argument("--normal-sample-rate", type=float, default=0.1)

    show = commands.add_parser("show", help="show a retained trace or dropped summary")
    show.add_argument("database")
    show.add_argument("trace_id")

    overview = commands.add_parser("overview", help="show aggregate sampling and latency data")
    overview.add_argument("database")

    services = commands.add_parser("services", help="show the retained service graph")
    services.add_argument("database")

    serve = commands.add_parser("serve", help="run the bounded JSON collector API")
    serve.add_argument("database")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument(
        "--token-env",
        default="TRACELENS_TOKEN",
        help="environment variable containing an optional bearer token",
    )

    demo = commands.add_parser("demo", help="run the deterministic sampling experiment")
    demo.add_argument("--database")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "demo":
            result: Any = run_demo(arguments.database)
        elif arguments.command == "init":
            store = TraceStore(arguments.database)
            result = {"database": str(store.database), "status": "initialized"}
        else:
            store = TraceStore(arguments.database)
            if arguments.command == "ingest":
                result = store.ingest_many(_load_spans(arguments.json_file))
            elif arguments.command == "finalize":
                if (
                    not math.isfinite(arguments.slow_threshold_ms)
                    or not 0 <= arguments.slow_threshold_ms <= 86_400_000
                ):
                    raise ValidationError("slow threshold must be finite and between 0 and one day")
                result = store.finalize(
                    arguments.trace_id,
                    slow_threshold_ns=int(arguments.slow_threshold_ms * 1_000_000),
                    normal_sample_rate=arguments.normal_sample_rate,
                )
            elif arguments.command == "show":
                result = store.get_trace(arguments.trace_id)
            elif arguments.command == "overview":
                result = store.overview()
            elif arguments.command == "services":
                result = store.service_graph()
            elif arguments.command == "serve":
                token = os.environ.get(arguments.token_env)
                server = create_server(
                    store,
                    host=arguments.host,
                    port=arguments.port,
                    bearer_token=token,
                )
                _print(
                    {
                        "host": arguments.host,
                        "port": server.server_address[1],
                        "status": "listening",
                    }
                )
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
                finally:
                    server.server_close()
                return 0
            else:  # pragma: no cover - argparse guarantees a known command.
                raise AssertionError(f"unhandled command: {arguments.command}")
        _print(result)
        if isinstance(result, dict):
            return 0 if result.get("success", True) else 1
        return 0
    except (TraceLensError, OSError, ValueError, sqlite3.Error) as exc:
        _print({"error": exc.__class__.__name__, "message": str(exc)}, stream=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
