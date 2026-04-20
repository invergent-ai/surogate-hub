"""Runtime configuration read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    hub_url: str
    hub_access_key: str
    hub_secret_key: str
    webhook_host: str = "0.0.0.0"
    webhook_port: int = 8080
    dataset_metadata_key: str = "type"
    dataset_metadata_value: str = "dataset"
    text_column_metadata_key: str = "dataset.text_column"
    stats_commit_marker: str = "stats_commit"
    source_commit_marker: str = "source_commit"
    stats_ref_prefix: str = "_stats_"
    parquet_ref_prefix: str = "_parquet_"
    parquet_conversion_marker: str = "parquet_conversion"
    producer_name: str = "surogate-hub-stats-worker"
    producer_version: str = "0.1.0"
    list_page_size: int = 1000
    max_in_flight: int = 4
    shared_secret: Optional[str] = None  # optional webhook auth token

    @classmethod
    def from_env(cls) -> "Config":
        required = {
            "hub_url": "SHUB_URL",
            "hub_access_key": "SHUB_ACCESS_KEY",
            "hub_secret_key": "SHUB_SECRET_KEY",
        }
        values = {}
        missing = []
        for field, env in required.items():
            v = os.environ.get(env)
            if not v:
                missing.append(env)
            else:
                values[field] = v
        if missing:
            raise RuntimeError(
                f"missing required environment variables: {', '.join(missing)}"
            )
        return cls(
            **values,
            webhook_host=os.environ.get("WORKER_HOST", "0.0.0.0"),
            webhook_port=int(os.environ.get("WORKER_PORT", "8080")),
            max_in_flight=int(os.environ.get("WORKER_MAX_IN_FLIGHT", "4")),
            shared_secret=os.environ.get("WORKER_SHARED_SECRET") or None,
        )
