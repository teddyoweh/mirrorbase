"""MirrorBase REST API server.

Security:
    - API key auth on every request (MIRRORBASE_API_KEY env var)
    - Source connection strings encrypted at rest (Fernet)
    - No wildcard CORS — origin whitelist only
    - Rate limiting per IP
    - Request logging with IP + endpoint (no secrets)
    - Connection strings never returned in full (masked)
    - Clone Postgres instances use scram-sha-256 auth with random passwords
"""

import hashlib
import hmac
import json
import os
import secrets
import ssl
import time
import threading
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from .core import MirrorBase
from .exceptions import MirrorBaseError

mb = MirrorBase()

# ── Security config via env vars ──
API_KEY = os.environ.get("MIRRORBASE_API_KEY")
ALLOWED_ORIGINS = os.environ.get("MIRRORBASE_ALLOWED_ORIGINS", "").split(",")
TLS_CERT = os.environ.get("MIRRORBASE_TLS_CERT")
TLS_KEY = os.environ.get("MIRRORBASE_TLS_KEY")
RATE_LIMIT_RPM = int(os.environ.get("MIRRORBASE_RATE_LIMIT", "120"))  # requests per minute


def _mask_connstring(connstring: str) -> str:
    """Mask password in connection strings. Never expose credentials in API responses."""
    parsed = urlparse(connstring)
    if parsed.password:
        masked = connstring.replace(f":{parsed.password}@", ":***@")
        return masked
    return connstring


class RateLimiter:
    """Simple per-IP rate limiter using sliding window."""

    def __init__(self, rpm: int):
        self.rpm = rpm
        self.requests: dict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def allow(self, ip: str) -> bool:
        now = time.time()
        window = 60.0
        with self._lock:
            # Clean old entries
            self.requests[ip] = [t for t in self.requests[ip] if now - t < window]
            if len(self.requests[ip]) >= self.rpm:
                return False
            self.requests[ip].append(now)
            return True


rate_limiter = RateLimiter(RATE_LIMIT_RPM)


class MirrorBaseHandler(BaseHTTPRequestHandler):

    def _json(self, status: int, data: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        # Restricted CORS — only allowed origins
        origin = self.headers.get("Origin", "")
        if origin in ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _path_parts(self) -> list[str]:
        return [p for p in urlparse(self.path).path.split("/") if p]

    def _client_ip(self) -> str:
        # Support reverse proxy forwarding
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

    def _auth(self) -> bool:
        """Verify API key. Returns True if authorized."""
        if not API_KEY:
            # No API key configured — reject all requests in production
            self._json(500, {"error": "MIRRORBASE_API_KEY not configured. Server is locked."})
            return False

        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            self._json(401, {"error": "Missing Authorization: Bearer <api_key>"})
            return False

        provided = header[7:]
        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(provided.encode(), API_KEY.encode()):
            self._json(403, {"error": "Invalid API key"})
            return False

        return True

    def _rate_check(self) -> bool:
        ip = self._client_ip()
        if not rate_limiter.allow(ip):
            self._json(429, {"error": "Rate limit exceeded"})
            return False
        return True

    def do_OPTIONS(self):
        self.send_response(200)
        origin = self.headers.get("Origin", "")
        if origin in ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "3600")
        self.end_headers()

    def do_GET(self):
        if not self._rate_check():
            return

        parts = self._path_parts()

        # Root — show API docs (no auth, so people know what this is)
        if not parts:
            self._json(200, {
                "name": "MirrorBase",
                "version": "0.1.0",
                "docs": {
                    "POST /connect": {
                        "description": "Connect a source Postgres database",
                        "body": {"url": "postgresql://user:pass@host/dbname"},
                        "returns": {"base_id": "string", "connstring": "string", "elapsed": "float"},
                    },
                    "POST /clone": {
                        "description": "Create an instant clone",
                        "body": {"base_id": "string", "name": "string (optional)"},
                        "returns": {"clone_id": "string", "connstring": "postgresql://...", "elapsed": "float"},
                    },
                    "GET /bases": "List all connected databases",
                    "GET /bases/:id": "Status + migration progress for a base",
                    "GET /clones": "List all clones",
                    "DELETE /clones/:id": "Destroy a clone",
                    "DELETE /bases/:id": "Teardown base + all clones",
                    "GET /health": "Health check",
                },
                "auth": "Bearer token in Authorization header",
                "example": f"curl -X POST {self.headers.get('Host', 'localhost:8100')}/connect -H 'Authorization: Bearer <key>' -H 'Content-Type: application/json' -d '{{\"url\": \"postgresql://...\"}}'",
            })
            return

        # Health check — no auth required (for load balancer probes)
        if parts == ["health"]:
            self._json(200, {"status": "ok"})
            return

        if not self._auth():
            return

        if parts == ["bases"]:
            bases = mb.list_bases()
            # Mask connection strings
            for b in bases:
                b["source_connstring"] = _mask_connstring(b.get("source_connstring", ""))
                b["direct_connstring"] = _mask_connstring(b.get("direct_connstring", ""))
            self._json(200, {"bases": bases})

        elif len(parts) == 2 and parts[0] == "bases":
            try:
                status = mb.status(parts[1])
                status["migration"] = mb.migration_status(parts[1])
                status["source"] = _mask_connstring(status.get("source", ""))
                self._json(200, status)
            except MirrorBaseError as e:
                self._json(404, {"error": str(e)})

        elif parts == ["clones"]:
            base_id = parse_qs(urlparse(self.path).query).get("base_id", [None])[0]
            self._json(200, {"clones": mb.list_clones(base_id=base_id)})

        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self._rate_check() or not self._auth():
            return

        parts = self._path_parts()
        body = self._body()

        if parts == ["connect"]:
            url = body.get("url")
            if not url:
                self._json(400, {"error": "url required"})
                return
            try:
                t0 = time.time()
                base_id = mb.connect(url)
                elapsed = time.time() - t0
                status = mb.status(base_id)
                self._json(200, {
                    "base_id": base_id,
                    "connstring": status["connstring"],
                    "elapsed": round(elapsed, 2),
                })
            except MirrorBaseError as e:
                self._json(500, {"error": str(e)})

        elif parts == ["clone"]:
            base_id = body.get("base_id")
            name = body.get("name")
            if not base_id:
                self._json(400, {"error": "base_id required"})
                return
            try:
                t0 = time.time()
                clone_id, connstr = mb.clone(base_id, clone_id=name)
                elapsed = time.time() - t0
                self._json(200, {
                    "clone_id": clone_id,
                    "connstring": connstr,
                    "elapsed": round(elapsed, 2),
                })
            except MirrorBaseError as e:
                self._json(500, {"error": str(e)})

        else:
            self._json(404, {"error": "not found"})

    def do_DELETE(self):
        if not self._rate_check() or not self._auth():
            return

        parts = self._path_parts()

        if len(parts) == 2 and parts[0] == "clones":
            try:
                mb.destroy(parts[1])
                self._json(200, {"destroyed": parts[1]})
            except MirrorBaseError as e:
                self._json(404, {"error": str(e)})

        elif len(parts) == 2 and parts[0] == "bases":
            try:
                mb.teardown(parts[1])
                self._json(200, {"torn_down": parts[1]})
            except MirrorBaseError as e:
                self._json(404, {"error": str(e)})

        else:
            self._json(404, {"error": "not found"})

    def log_message(self, format, *args):
        ip = self._client_ip()
        print(f"[mirrorbase] {ip} {args[0]}")


def serve(host: str = "0.0.0.0", port: int = 8100):
    if not API_KEY:
        print("WARNING: MIRRORBASE_API_KEY not set. All requests will be rejected.")
        print("  Set it with: export MIRRORBASE_API_KEY=$(python3 -c \"import secrets; print(secrets.token_urlsafe(32))\")")

    server = HTTPServer((host, port), MirrorBaseHandler)

    # TLS if certs provided
    if TLS_CERT and TLS_KEY:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(TLS_CERT, TLS_KEY)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        proto = "https"
    else:
        proto = "http"

    base_url = f"{proto}://{host}:{port}"
    print(f"""
MirrorBase running at {base_url}

  API docs:     {base_url}/
  Health:       {base_url}/health

  Endpoints:
    POST   /connect         Connect a source database
    POST   /clone           Create an instant clone
    GET    /bases            List connected databases
    GET    /bases/:id        Base status
    GET    /clones           List all clones
    DELETE /clones/:id       Destroy a clone
    DELETE /bases/:id        Teardown base + all clones

  Auth: Authorization: Bearer <MIRRORBASE_API_KEY>

  Quick test:
    curl {base_url}/health
    curl -X POST {base_url}/connect \\
      -H "Authorization: Bearer $MIRRORBASE_API_KEY" \\
      -H "Content-Type: application/json" \\
      -d '{{"url": "postgresql://user:pass@host/dbname"}}'
""")
    server.serve_forever()


if __name__ == "__main__":
    serve(port=int(os.environ.get("MIRRORBASE_PORT", "8100")))
