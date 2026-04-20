"""Event processor.

For each eligible post-commit event:
  1. Convert the dataset to parquet via HF ``datasets`` over our ``shub://``
     fsspec filesystem, commit to ``_parquet/<data_ref>``.
  2. Run stats computers against that parquet branch, commit to
     ``_stats/<data_ref>``.

Conversion is skipped-not-failed: if ``datasets`` can't load the repo
(unsupported layout / malformed config), we log and skip stats for that
commit. Crashing would leave the webhook retrying forever.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import duckdb
import fsspec

from surogate_hub_worker import computers
from surogate_hub_worker.computers import Artifact
from surogate_hub_worker.config import Config
from surogate_hub_worker.converter import ConversionResult, DatasetConverter
from surogate_hub_worker.fs import PROTOCOL
from surogate_hub_worker.hub import HubClient
from surogate_hub_worker.writer import (
    ParquetBranchWriter,
    StatsBranchWriter,
    WriteResult,
)


log = logging.getLogger(__name__)

POST_COMMIT_EVENT = "post-commit"


@dataclass
class Event:
    repository: str
    branch: str
    commit_id: str
    event_type: str
    commit_metadata: Dict[str, str]

    @classmethod
    def from_payload(cls, payload: Dict) -> "Event":
        required = ("event_type", "repository_id", "branch_id", "commit_id")
        missing = [k for k in required if not payload.get(k)]
        if missing:
            raise ValueError(f"webhook payload missing fields: {missing}")
        return cls(
            repository=payload["repository_id"],
            branch=payload["branch_id"],
            commit_id=payload["commit_id"],
            event_type=payload["event_type"],
            commit_metadata=dict(payload.get("commit_metadata") or {}),
        )


class Skip(Exception):
    """Raised when an event is not eligible for processing."""


class EventProcessor:
    def __init__(self, config: Config, hub: HubClient) -> None:
        self._config = config
        self._hub = hub
        self._converter = DatasetConverter(config)
        self._parquet_writer = ParquetBranchWriter(config, hub)
        self._stats_writer = StatsBranchWriter(config, hub)

    def process(self, event: Event) -> Optional[WriteResult]:
        repo_md = self._check_eligible(event)
        text_column = repo_md.get(self._config.text_column_metadata_key) or None

        conversion = self._converter.convert(event.repository, event.commit_id)
        if not conversion.splits:
            log.info(
                "no parquet produced from %s@%s (configs=%s, error=%s); skipping",
                event.repository, event.commit_id,
                conversion.configs_seen, conversion.error,
            )
            return None

        parquet_result = self._parquet_writer.write(
            repository=event.repository,
            data_ref=event.branch,
            source_commit=event.commit_id,
            splits=conversion.splits,
        )
        log.info(
            "committed %d parquet shard(s) to %s@%s (commit=%s)",
            len(parquet_result.written), event.repository,
            parquet_result.parquet_ref, parquet_result.parquet_commit_id,
        )

        urls = self._hub.shub_urls_for(
            event.repository, parquet_result.parquet_commit_id,
        )
        artifacts = self._compute_all(urls, text_column=text_column)
        stats_result = self._stats_writer.write(
            repository=event.repository,
            data_ref=event.branch,
            source_commit=event.commit_id,
            artifacts=artifacts,
        )
        log.info(
            "wrote %d stat artifacts to %s@%s (commit=%s)",
            len(stats_result.written), event.repository, stats_result.stats_ref,
            stats_result.stats_commit_id,
        )
        return stats_result

    def _check_eligible(self, event: Event) -> Dict[str, str]:
        if event.event_type != POST_COMMIT_EVENT:
            raise Skip(f"not a post-commit event: {event.event_type}")
        if event.branch.startswith(self._config.stats_ref_prefix):
            raise Skip(f"commit on stats branch {event.branch}")
        if event.branch.startswith(self._config.parquet_ref_prefix):
            raise Skip(f"commit on parquet conversion branch {event.branch}")
        if event.commit_metadata.get(self._config.stats_commit_marker) == "true":
            raise Skip(f"self-originated stats commit {event.commit_id}")
        if event.commit_metadata.get(self._config.parquet_conversion_marker) == "true":
            raise Skip(f"self-originated parquet commit {event.commit_id}")
        repo_md = self._hub.get_repo_metadata(event.repository)
        value = repo_md.get(self._config.dataset_metadata_key)
        if value != self._config.dataset_metadata_value:
            raise Skip(
                f"repo {event.repository} has "
                f"{self._config.dataset_metadata_key}={value!r}, "
                f"expected {self._config.dataset_metadata_value!r}"
            )
        return repo_md

    def _compute_all(
        self, urls: List[str], *, text_column: Optional[str],
    ) -> List[Artifact]:
        con = duckdb.connect(database=":memory:")
        try:
            # Routes ``read_parquet('shub://...')`` through our SDK-authed
            # fsspec FS. Without this, DuckDB falls back to httpfs+presign,
            # which fails against the local blockstore (presign unsupported).
            con.register_filesystem(fsspec.filesystem(
                PROTOCOL,
                host=self._config.hub_url,
                username=self._config.hub_access_key,
                password=self._config.hub_secret_key,
            ))
            computers.register_view(con, urls)
            out: List[Artifact] = [
                computers.compute_summary(con),
                computers.compute_duplicates(con),
            ]
            if text_column:
                out.append(computers.compute_pii(con, text_column=text_column))
                out.append(computers.compute_token_lengths(con, text_column=text_column))
            else:
                log.info("no text_column set; skipping pii and token_lengths")
            return out
        finally:
            con.close()
