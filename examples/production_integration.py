"""
Production Integration Example
===============================

Option A: Direct (MirrorBase runs on same server as your backend)
Option B: Client (MirrorBase runs as a separate service)
"""

# ─── Option A: Direct (same server) ───

import mirrorbase

mb = mirrorbase.MirrorBase()


def on_customer_onboard(customer_db_url: str) -> str:
    """Customer connects their database. Called once during onboarding."""
    base_id = mb.connect(customer_db_url)
    return base_id


def on_agent_start(base_id: str):
    """Agent needs database access. Called every time an agent runs."""
    clone_id, pg_url = mb.clone(base_id)
    return clone_id, pg_url


def on_agent_done(clone_id: str):
    """Agent finished. Destroy the sandbox."""
    mb.destroy(clone_id)


# ─── Option B: Client (separate service) ───

from mirrorbase.client import MirrorBaseClient

client = MirrorBaseClient(
    url="https://mirrorbase.yourcompany.com:8100",
    api_key="your-api-key",
)


def on_customer_onboard_remote(customer_db_url: str) -> str:
    result = client.connect(customer_db_url)
    return result["base_id"]


def on_agent_start_remote(base_id: str):
    result = client.clone(base_id)
    return result["clone_id"], result["connstring"]


def on_agent_done_remote(clone_id: str):
    client.destroy(clone_id)
