"""Webhook receiver: payload validation, auth header, and async dispatch."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from surogate_hub_worker.config import Config
from surogate_hub_worker.server import create_app


def _config(shared_secret=None):
    return Config(
        hub_url="http://hub",
        hub_access_key="AK",
        hub_secret_key="SK",
        shared_secret=shared_secret,
    )


def _client(cfg, processor):
    app = create_app(cfg, hub=MagicMock(), processor=processor)
    return TestClient(app)


def test_healthz():
    with _client(_config(), MagicMock()) as c:
        assert c.get("/healthz").json() == {"status": "ok"}


def test_webhook_rejects_non_object():
    with _client(_config(), MagicMock()) as c:
        r = c.post("/webhook", json=["nope"])
        assert r.status_code == 400


def test_webhook_auth_header_required_when_secret_set():
    with _client(_config(shared_secret="top-secret"), MagicMock()) as c:
        r = c.post("/webhook", json={"event_type": "post-commit"})
        assert r.status_code == 401

        r = c.post(
            "/webhook",
            json={"event_type": "post-commit"},
            headers={"x-stats-worker-secret": "top-secret"},
        )
        assert r.status_code == 202


def test_webhook_dispatches_to_processor():
    processor = MagicMock()
    with _client(_config(), processor) as c:
        payload = {
            "event_type": "post-commit",
            "repository_id": "r",
            "branch_id": "main",
            "commit_id": "abc",
            "commit_metadata": {},
        }
        r = c.post("/webhook", json=payload)
        assert r.status_code == 202
        # Background thread may not have fired yet — give it a moment.
        for _ in range(20):
            if processor.process.called:
                break
            time.sleep(0.05)
        assert processor.process.called
