"""Tests for the REST API server — auth, rate limiting, masking."""

import json
import os
import threading
import time
import urllib.request
import urllib.error
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def api_server():
    """Start a test server on a random port."""
    os.environ["MIRRORBASE_API_KEY"] = "test-key-123"
    os.environ["MIRRORBASE_ALLOWED_ORIGINS"] = "https://test.com"

    from mirrorbase.server import MirrorBaseHandler
    from http.server import HTTPServer

    server = HTTPServer(("127.0.0.1", 0), MirrorBaseHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def _get(url, path, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    req = urllib.request.Request(f"{url}{path}", headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _post(url, path, body, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    req = urllib.request.Request(
        f"{url}{path}",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_health_no_auth(api_server):
    status, body = _get(api_server, "/health")
    assert status == 200
    assert body["status"] == "ok"


def test_root_shows_docs(api_server):
    status, body = _get(api_server, "/")
    assert status == 200
    assert body["name"] == "MirrorBase"
    assert "POST /connect" in body["docs"]


def test_no_auth_rejected(api_server):
    status, body = _get(api_server, "/bases")
    assert status == 401
    assert "Missing" in body["error"]


def test_wrong_key_rejected(api_server):
    status, body = _get(api_server, "/bases", key="wrong-key")
    assert status == 403
    assert "Invalid" in body["error"]


def test_correct_key_works(api_server):
    status, body = _get(api_server, "/bases", key="test-key-123")
    assert status == 200
    assert "bases" in body


def test_connect_requires_url(api_server):
    status, body = _post(api_server, "/connect", {}, key="test-key-123")
    assert status == 400
    assert "url required" in body["error"]


def test_404_on_unknown_path(api_server):
    status, body = _get(api_server, "/nonexistent", key="test-key-123")
    assert status == 404
