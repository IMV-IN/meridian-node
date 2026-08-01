"""Dev/reference control-plane HTTP server (stdlib only).

Wraps ControlCore over real HTTP so the agent's real HttpTransport and
managed-host loop can be driven end to end without the production control plane.
It is a development and CI tool: no persistence, no TLS by default, no real CA.
The production `meridian-control` service lives in the central Meridian repo.

Run:  python -m meridian_node.devserver --port 8443 --token dev-token:auto
Admin (dev only): POST /admin/claims/{id}/approve,
                  POST /admin/nodes/{id}/desired  (body = snapshot),
                  POST /admin/nodes/{id}/stop-authorize  (body = {"engine_ids":[...]})
"""

from __future__ import annotations

import base64
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .refcontrol import ControlCore, ControlError, Token

_ENROLL = re.compile(r"^/control/v1/enroll$")
_CLAIM = re.compile(r"^/control/v1/enroll/claims/([^/]+)$")
_SESSION = re.compile(r"^/control/v1/nodes/([^/]+)/sessions$")
_HEARTBEAT = re.compile(r"^/control/v1/nodes/([^/]+)/heartbeat$")
_DESIRED = re.compile(r"^/control/v1/nodes/([^/]+)/desired-state$")
_OBSERVE = re.compile(r"^/control/v1/nodes/([^/]+)/observations$")
_ADMIN_APPROVE = re.compile(r"^/admin/claims/([^/]+)/approve$")
_ADMIN_DESIRED = re.compile(r"^/admin/nodes/([^/]+)/desired$")
_ADMIN_STOP = re.compile(r"^/admin/nodes/([^/]+)/stop-authorize$")


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def make_handler(core: ControlCore):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # quiet by default
            pass

        def _send(self, status: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _error(self, e: ControlError) -> None:
            from datetime import datetime, timezone

            self._send(e.http_status, {"error": {
                "code": e.code, "message": e.message, "retryable": e.retryable,
                "server_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }})

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            return dict(json.loads(self.rfile.read(length) or b"{}"))

        def _token(self):
            auth = self.headers.get("Authorization", "")
            return auth[7:] if auth.startswith("Bearer ") else None

        def do_GET(self):
            try:
                path = urlparse(self.path).path
                m = _CLAIM.match(path)
                if m:
                    claim_id = m.group(1)
                    proof = self.headers.get("X-Possession-Proof")
                    if proof is None:
                        self._send(200, {"status": "pending", "claim_id": claim_id,
                                         "nonce": core.claim_nonce(claim_id)})
                    else:
                        self._send(200, core.resolve_claim(claim_id, _b64url_decode(proof)))
                    return
                m = _DESIRED.match(path)
                if m:
                    gen = int(parse_qs(urlparse(self.path).query).get("generation", ["0"])[0])
                    self._send(200, core.fetch_desired_state(m.group(1), gen))
                    return
                self._send(404, {"error": {"code": "NODE_NOT_FOUND", "message": "no route",
                                           "retryable": False, "server_time": ""}})
            except ControlError as e:
                self._error(e)

        def do_POST(self):
            try:
                path = urlparse(self.path).path
                body = self._body()
                if _ENROLL.match(path):
                    self._send(200, core.enroll(self._token(), body))
                    return
                m = _SESSION.match(path)
                if m:
                    self._send(200, core.establish_session(m.group(1), body))
                    return
                m = _HEARTBEAT.match(path)
                if m:
                    self._send(200, core.heartbeat(m.group(1), body))
                    return
                m = _OBSERVE.match(path)
                if m:
                    self._send(200, core.post_observation(m.group(1), body))
                    return
                m = _ADMIN_APPROVE.match(path)
                if m:
                    core.approve_claim(m.group(1))
                    self._send(200, {"ok": True})
                    return
                m = _ADMIN_DESIRED.match(path)
                if m:
                    core.set_desired(m.group(1), body)
                    self._send(200, {"ok": True})
                    return
                m = _ADMIN_STOP.match(path)
                if m:
                    core.stop_authorized[m.group(1)] = set(body.get("engine_ids", []))
                    self._send(200, {"ok": True})
                    return
                self._send(404, {"error": {"code": "NODE_NOT_FOUND", "message": "no route",
                                           "retryable": False, "server_time": ""}})
            except ControlError as e:
                self._error(e)

    return Handler


def build_server(core: ControlCore, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(core))


def _parse_tokens(specs: list[str]) -> dict[str, Token]:
    tokens: dict[str, Token] = {}
    for spec in specs:
        name, _, kind = spec.partition(":")
        tokens[name] = Token(auto_approve=(kind == "auto"))
    return tokens


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="meridian-node-devserver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--token", action="append", default=[],
                        help="token spec name[:auto]; repeatable")
    parser.add_argument("--lease-ttl", type=int, default=30)
    args = parser.parse_args(argv)

    tokens = _parse_tokens(args.token or ["dev-token:auto"])
    core = ControlCore(tokens, lease_ttl_seconds=args.lease_ttl)
    server = build_server(core, args.host, args.port)
    print(f"dev control plane on http://{args.host}:{args.port} (tokens: {list(tokens)})")
    print("WARNING: development server; not the production control plane.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
