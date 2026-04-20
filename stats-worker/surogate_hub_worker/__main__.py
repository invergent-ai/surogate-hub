"""Entry point: ``python -m surogate_hub_worker``."""

from __future__ import annotations

import logging
import sys

import uvicorn

from surogate_hub_worker.config import Config
from surogate_hub_worker.server import create_app


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    cfg = Config.from_env()
    uvicorn.run(
        create_app(cfg),
        host=cfg.webhook_host,
        port=cfg.webhook_port,
        log_config=None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
