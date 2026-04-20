"""FastAPI webhook receiver.

Registers at ``POST /webhook`` for Surogate Hub ``post-commit`` events.
Returns 202 immediately and processes the event on a bounded thread pool —
commits for 100GB datasets can take a long time, so we never block the
caller. Startup/shutdown manages the :class:`HubClient` lifetime.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request, status

from surogate_hub_worker.config import Config
from surogate_hub_worker.hub import HubClient
from surogate_hub_worker.processor import Event, EventProcessor, Skip


log = logging.getLogger(__name__)


def _run_event(processor: EventProcessor, payload: Dict[str, Any]) -> None:
    try:
        event = Event.from_payload(payload)
        processor.process(event)
    except Skip as exc:
        log.info("skipped event: %s", exc)
    except Exception:
        log.exception("stats computation failed")


def create_app(
    config: Optional[Config] = None,
    *,
    hub: Optional[HubClient] = None,
    processor: Optional[EventProcessor] = None,
) -> FastAPI:
    """Build the FastAPI app.

    In production, pass only ``config`` and the app constructs a
    :class:`HubClient` + :class:`EventProcessor` during lifespan. Tests can
    inject pre-built substitutes via ``hub`` / ``processor``.
    """
    resolved = config or Config.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app_hub = hub or HubClient(resolved)
        app_processor = processor or EventProcessor(resolved, app_hub)
        pool = ThreadPoolExecutor(
            max_workers=resolved.max_in_flight,
            thread_name_prefix="stats-worker",
        )
        app.state.config = resolved
        app.state.hub = app_hub
        app.state.processor = app_processor
        app.state.pool = pool
        try:
            yield
        finally:
            pool.shutdown(wait=True)

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
    async def webhook(
        request: Request,
        x_stats_worker_secret: Optional[str] = Header(default=None),
    ) -> Dict[str, str]:
        cfg: Config = request.app.state.config
        if cfg.shared_secret and x_stats_worker_secret != cfg.shared_secret:
            raise HTTPException(status_code=401, detail="invalid shared secret")
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="payload must be a JSON object")
        pool: ThreadPoolExecutor = request.app.state.pool
        processor: EventProcessor = request.app.state.processor
        pool.submit(_run_event, processor, payload)
        return {"status": "accepted"}

    return app
