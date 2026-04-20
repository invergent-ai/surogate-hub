"""Write computed stats to the parallel ``_stats/<data_ref>`` branch.

Uploads artifacts, merges their entries into ``manifest.json`` (preserving
entries for stats that weren't recomputed this run), and commits with a
``stats_commit=true`` metadata marker so downstream tooling can filter
out the worker's own commits.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from surogate_hub_sdk.stats.manifest import (
    MANIFEST_FILENAME,
    ManifestError,
    StatEntry,
    StatsManifest,
)

from surogate_hub_worker.computers import Artifact
from surogate_hub_worker.config import Config
from surogate_hub_worker.converter import ConvertedSplit
from surogate_hub_worker.hub import HubClient


DATASET_DICT_FILENAME = "dataset_dict.json"


log = logging.getLogger(__name__)


@dataclass
class WriteResult:
    stats_ref: str
    stats_commit_id: str
    written: List[str]


@dataclass
class ParquetWriteResult:
    parquet_ref: str
    parquet_commit_id: str
    written: List[str]


class ParquetBranchWriter:
    """Commits converted parquet shards to ``_parquet/<data_ref>``."""

    def __init__(self, config: Config, hub: HubClient) -> None:
        self._config = config
        self._hub = hub

    def ref_for(self, data_ref: str) -> str:
        return f"{self._config.parquet_ref_prefix}{data_ref}"

    def _clear_stale(self, repository: str, branch: str, keep: set) -> None:
        """Remove any path on ``branch`` we're not about to re-write.

        The parquet branch is worker-owned — on first creation it inherits
        every object from the source data ref, and between runs it can
        accumulate stats from a previous layout. Wiping the complement of
        ``keep`` on each run guarantees the branch contains only our
        current output.
        """
        stale = [
            obj.path
            for obj in self._hub.list_all_objects(repository, branch)
            if obj.path not in keep
        ]
        if stale:
            self._hub.delete_objects(repository, branch, stale)
            log.info(
                "cleared %d stale object(s) from %s", len(stale), branch,
            )

    def write(
        self,
        repository: str,
        data_ref: str,
        source_commit: str,
        splits: List[ConvertedSplit],
    ) -> ParquetWriteResult:
        parquet_ref = self.ref_for(data_ref)
        self._hub.ensure_branch(repository, parquet_ref, source=data_ref)

        keep_paths = {DATASET_DICT_FILENAME}
        for split in splits:
            keep_paths.add(split.path())
            keep_paths.add(split.info_path())
        self._clear_stale(repository, parquet_ref, keep_paths)

        written: List[str] = []
        for split in splits:
            path = split.path()
            self._hub.upload(repository, parquet_ref, path, split.content)
            written.append(path)
            self._hub.upload(
                repository, parquet_ref, split.info_path(),
                split.dataset_info_json(repository),
            )
            written.append(split.info_path())
        self._hub.upload(
            repository, parquet_ref, DATASET_DICT_FILENAME,
            _dataset_dict_bytes([s.split_name for s in splits]),
        )
        written.append(DATASET_DICT_FILENAME)
        commit = self._hub.commit(
            repository=repository,
            branch=parquet_ref,
            message=f"parquet conversion of {source_commit[:12]}",
            metadata={
                self._config.parquet_conversion_marker: "true",
                self._config.source_commit_marker: source_commit,
            },
        )
        return ParquetWriteResult(
            parquet_ref=parquet_ref,
            parquet_commit_id=commit.id,
            written=written,
        )


class StatsBranchWriter:
    def __init__(self, config: Config, hub: HubClient) -> None:
        self._config = config
        self._hub = hub

    def stats_ref_for(self, data_ref: str) -> str:
        return f"{self._config.stats_ref_prefix}{data_ref}"

    def write(
        self,
        repository: str,
        data_ref: str,
        source_commit: str,
        artifacts: List[Artifact],
    ) -> WriteResult:
        stats_ref = self.stats_ref_for(data_ref)
        self._hub.ensure_branch(repository, stats_ref, source=data_ref)

        existing = self._load_existing_manifest(repository, stats_ref)
        new_entries = {
            artifact.stat_name: self._entry_for(artifact, source_commit=source_commit)
            for artifact in artifacts
        }
        manifest = StatsManifest(
            stats={**existing.stats, **new_entries},
            manifest_version=existing.manifest_version,
        )

        written: List[str] = []
        for artifact in artifacts:
            path = new_entries[artifact.stat_name].data_path()
            self._hub.upload(repository, stats_ref, path, artifact.content)
            written.append(path)

        self._hub.upload(
            repository,
            stats_ref,
            MANIFEST_FILENAME,
            manifest.to_json().encode("utf-8"),
        )
        written.append(MANIFEST_FILENAME)

        commit = self._hub.commit(
            repository=repository,
            branch=stats_ref,
            message=f"stats for {source_commit[:12]}",
            metadata={
                self._config.stats_commit_marker: "true",
                self._config.source_commit_marker: source_commit,
            },
        )
        return WriteResult(
            stats_ref=stats_ref,
            stats_commit_id=commit.id,
            written=written,
        )

    def _load_existing_manifest(self, repository: str, stats_ref: str) -> StatsManifest:
        raw = self._hub.read_object(repository, stats_ref, MANIFEST_FILENAME)
        if raw is None:
            return StatsManifest(stats={})
        try:
            return StatsManifest.from_json(raw.decode("utf-8"))
        except ManifestError as exc:
            log.warning(
                "existing manifest at %s is invalid (%s); rebuilding from scratch",
                stats_ref, exc,
            )
            return StatsManifest(stats={})

    def _entry_for(self, artifact: Artifact, *, source_commit: str) -> StatEntry:
        return StatEntry(
            name=artifact.stat_name,
            version=artifact.version,
            source_commit=source_commit,
            produced_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            producer=f"{self._config.producer_name}@{self._config.producer_version}",
            row_count=artifact.row_count,
            data_format=artifact.data_format,
            parameters=artifact.parameters,
        )


def _dataset_dict_bytes(split_names: List[str]) -> bytes:
    return json.dumps({"splits": list(split_names)}, indent=2).encode("utf-8")
