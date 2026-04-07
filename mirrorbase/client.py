"""MirrorBase client for calling the REST API from Spawn's backend.

Usage:
    from mirrorbase.client import MirrorBaseClient

    client = MirrorBaseClient(
        url="https://mirrorbase.spawnlabs.ai",
        api_key="your-api-key",
    )

    # Customer onboards
    base = client.connect("postgresql://customer:pass@host/db")

    # Agent needs a sandbox
    clone = client.clone(base["base_id"])
    agent_db_url = clone["connstring"]

    # Agent done
    client.destroy(clone["clone_id"])
"""

import json
import urllib.request
import urllib.error
from typing import Optional


class MirrorBaseClient:
    """HTTP client for the MirrorBase REST API."""

    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.api_key = api_key

    def _request(self, method: str, path: str, body: dict = None) -> dict:
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(
            f"{self.url}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            error = json.loads(e.read())
            raise Exception(f"MirrorBase API error ({e.code}): {error.get('error', 'unknown')}")

    def health(self) -> dict:
        return self._request("GET", "/health")

    def connect(self, database_url: str) -> dict:
        """Connect to a source database. Returns {"base_id", "connstring", "elapsed"}."""
        return self._request("POST", "/connect", {"url": database_url})

    def clone(self, base_id: str, name: Optional[str] = None) -> dict:
        """Create a clone. Returns {"clone_id", "connstring", "elapsed"}."""
        body = {"base_id": base_id}
        if name:
            body["name"] = name
        return self._request("POST", "/clone", body)

    def status(self, base_id: str) -> dict:
        """Get base status + migration progress."""
        return self._request("GET", f"/bases/{base_id}")

    def list_bases(self) -> list[dict]:
        return self._request("GET", "/bases")["bases"]

    def list_clones(self, base_id: Optional[str] = None) -> list[dict]:
        path = "/clones"
        if base_id:
            path += f"?base_id={base_id}"
        return self._request("GET", path)["clones"]

    def destroy(self, clone_id: str) -> dict:
        """Destroy a clone."""
        return self._request("DELETE", f"/clones/{clone_id}")

    def teardown(self, base_id: str) -> dict:
        """Tear down a base and all its clones."""
        return self._request("DELETE", f"/bases/{base_id}")
