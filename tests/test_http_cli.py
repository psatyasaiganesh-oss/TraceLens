from __future__ import annotations

import contextlib
import http.client
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path

from tracelens.__main__ import main
from tracelens.httpserver import create_server
from tracelens.store import TraceStore

from .helpers import TRACE_A, basic_trace


class HTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = TraceStore(Path(self.temporary.name) / "traces.sqlite3")
        self.token = "a-secure-test-token"
        self.server = create_server(
            self.store,
            port=0,
            bearer_token=self.token,
            max_body_bytes=1_024,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        *,
        authorized: bool = True,
        content_type: str = "application/json",
    ) -> tuple[int, dict[str, object]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        headers = {"Content-Type": content_type}
        if authorized:
            headers["Authorization"] = f"Bearer {self.token}"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        status = response.status
        connection.close()
        return status, payload

    def test_health_is_public_but_data_requires_authentication(self) -> None:
        status, payload = self.request("GET", "/health", authorized=False)
        self.assertEqual((status, payload), (200, {"status": "ok"}))
        status, payload = self.request("GET", "/v1/overview", authorized=False)
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "PermissionError")

    def test_ingest_finalize_and_query_round_trip(self) -> None:
        spans = [span.to_dict() for span in basic_trace()]
        status, intake = self.request(
            "POST", "/v1/spans", json.dumps({"spans": spans}).encode()
        )
        self.assertEqual(status, 202)
        self.assertEqual(intake["accepted"], 2)
        status, report = self.request(
            "POST",
            f"/v1/traces/{TRACE_A}/finalize",
            json.dumps({"normal_sample_rate": 1.0, "slow_threshold_ms": 100}).encode(),
        )
        self.assertEqual(status, 200)
        self.assertTrue(report["kept"])
        status, trace = self.request("GET", f"/v1/traces/{TRACE_A}")
        self.assertEqual(status, 200)
        self.assertEqual(trace["stored_span_count"], 2)
        status, overview = self.request("GET", "/v1/overview")
        self.assertEqual(status, 200)
        self.assertEqual(overview["finalized_traces"], 1)

    def test_bad_content_type_and_json_are_rejected(self) -> None:
        status, payload = self.request(
            "POST", "/v1/spans", b"{}", content_type="text/plain"
        )
        self.assertEqual(status, 400)
        self.assertIn("Content-Type", payload["message"])
        status, payload = self.request("POST", "/v1/spans", b"{broken")
        self.assertEqual(status, 400)
        self.assertIn("UTF-8 JSON", payload["message"])

    def test_schema_and_unknown_route_are_rejected(self) -> None:
        status, _ = self.request("POST", "/v1/spans", b"{}")
        self.assertEqual(status, 400)
        status, payload = self.request("GET", "/v1/missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "not_found")

    def test_oversized_body_is_rejected(self) -> None:
        status, payload = self.request("POST", "/v1/spans", b"x" * 1_025)
        self.assertEqual(status, 413)
        self.assertEqual(payload["error"], "BodyTooLargeError")

    def test_non_loopback_listener_requires_token(self) -> None:
        with self.assertRaisesRegex(Exception, "bearer token"):
            create_server(self.store, host="0.0.0.0", port=0)

    def test_server_configuration_is_bounded(self) -> None:
        with self.assertRaisesRegex(Exception, "max_body_bytes"):
            create_server(self.store, port=0, max_body_bytes=100)
        with self.assertRaisesRegex(Exception, "16 to 512"):
            create_server(self.store, port=0, bearer_token="short")


class CLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def invoke(self, *arguments: str) -> tuple[int, dict[str, object], str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(arguments))
        output = json.loads(stdout.getvalue()) if stdout.getvalue() else {}
        return code, output, stderr.getvalue()

    def test_init_ingest_finalize_and_overview(self) -> None:
        database = self.root / "trace.sqlite3"
        source = self.root / "spans.json"
        source.write_text(
            json.dumps({"spans": [span.to_dict() for span in basic_trace()]}),
            encoding="utf-8",
        )
        code, initialized, _ = self.invoke("init", str(database))
        self.assertEqual(code, 0)
        self.assertEqual(initialized["status"], "initialized")
        code, intake, _ = self.invoke("ingest", str(database), str(source))
        self.assertEqual(code, 0)
        self.assertEqual(intake["accepted"], 2)
        code, report, _ = self.invoke(
            "finalize", str(database), TRACE_A, "--normal-sample-rate", "1"
        )
        self.assertEqual(code, 0)
        self.assertTrue(report["kept"])
        code, overview, _ = self.invoke("overview", str(database))
        self.assertEqual(code, 0)
        self.assertEqual(overview["finalized_traces"], 1)

    def test_cli_returns_structured_validation_error(self) -> None:
        database = self.root / "trace.sqlite3"
        code, output, error = self.invoke("show", str(database), "bad-id")
        self.assertEqual(code, 2)
        self.assertEqual(output, {})
        payload = json.loads(error)
        self.assertEqual(payload["error"], "ValidationError")

    def test_cli_rejects_nonfinite_slow_threshold(self) -> None:
        database = self.root / "trace.sqlite3"
        TraceStore(database).ingest_many(basic_trace())
        code, _, error = self.invoke(
            "finalize", str(database), TRACE_A, "--slow-threshold-ms", "nan"
        )
        self.assertEqual(code, 2)
        self.assertIn("must be finite", json.loads(error)["message"])


if __name__ == "__main__":
    unittest.main()
