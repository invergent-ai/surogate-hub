"""Read precomputed dataset stats from a parallel stats branch.

Piggy-backs on :class:`surogate_hub_sdk.parquet.ParquetQuery` so that reading
a multi-GB duplicates table with a ``min_count`` filter only pulls the rows
the caller actually asks for.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from surogate_hub_sdk.api.objects_api import ObjectsApi
from surogate_hub_sdk.api.refs_api import RefsApi
from surogate_hub_sdk.api_client import ApiClient
from surogate_hub_sdk.exceptions import NotFoundException
from surogate_hub_sdk.stats.manifest import (
    MANIFEST_FILENAME,
    DataFormat,
    ManifestError,
    StatEntry,
    StatsManifest,
)

if TYPE_CHECKING:
    import pyarrow


STATS_REF_PREFIX = "_stats_"

STAT_TOKEN_LENGTHS = "token_lengths"
STAT_DUPLICATES = "duplicates"
STAT_PII = "pii"
STAT_SUMMARY = "summary"


class DatasetStatsError(Exception):
    pass


class Freshness(str, enum.Enum):
    FRESH = "fresh"
    STALE = "stale"
    MISSING = "missing"


@dataclass
class FreshnessReport:
    stat_name: str
    state: Freshness
    source_commit: Optional[str] = None
    current_commit: Optional[str] = None

    @property
    def is_fresh(self) -> bool:
        return self.state is Freshness.FRESH


class DatasetStats:
    """Authoritative manifest-backed reader for dataset stats.

    Stats live on a parallel branch (default ``_stats_<data_ref>``), so they
    don't clutter the data branch history. The manifest at ``manifest.json``
    on that stats ref drives everything — a stat not in the manifest does
    not exist from the SDK's point of view.

    When ``ref`` is a commit SHA (immutable), pass an explicit ``stats_ref``
    since the default derivation only makes sense for branch/tag names.
    """

    def __init__(
        self,
        api_client: Optional[ApiClient] = None,
        *,
        repository: str,
        ref: str,
        stats_ref: Optional[str] = None,
    ) -> None:
        self._api_client = api_client or ApiClient.get_default()
        self._objects = ObjectsApi(self._api_client)
        self._refs = RefsApi(self._api_client)
        self.repository = repository
        self.ref = ref
        self.stats_ref = stats_ref or f"{STATS_REF_PREFIX}{ref}"
        self._manifest: Optional[StatsManifest] = None
        self._current_commit_cache: Optional[str] = None
        self._parquet_query_cache = None

    def manifest(self, *, reload: bool = False) -> StatsManifest:
        if self._manifest is None or reload:
            self._manifest = self._load_manifest()
        return self._manifest

    def list(self) -> List[str]:
        return sorted(self.manifest().stats.keys())

    def entry(self, name: str) -> StatEntry:
        stats = self.manifest().stats
        if name not in stats:
            raise DatasetStatsError(
                f"no stat named {name!r} in manifest at "
                f"{self.repository}@{self.stats_ref}:{MANIFEST_FILENAME}"
            )
        return stats[name]

    def freshness(self, name: str, *, reload: bool = False) -> FreshnessReport:
        try:
            entry = self.entry(name)
        except DatasetStatsError:
            return FreshnessReport(stat_name=name, state=Freshness.MISSING)
        current = self._current_commit(reload=reload)
        state = Freshness.FRESH if entry.source_commit == current else Freshness.STALE
        return FreshnessReport(
            stat_name=name,
            state=state,
            source_commit=entry.source_commit,
            current_commit=current,
        )

    def summary(self, name: str = STAT_SUMMARY) -> Dict[str, Any]:
        entry = self.entry(name)
        if entry.data_format is not DataFormat.JSON:
            raise DatasetStatsError(
                f"stat {name!r} has data_format={entry.data_format.value!r}, "
                f"expected 'json'. Use read()/sql() for parquet stats."
            )
        raw = self._objects.get_object(
            repository=self.repository,
            ref=self.stats_ref,
            path=entry.data_path(),
        )
        return json.loads(bytes(raw).decode("utf-8"))

    def read(
        self,
        name: str,
        *,
        columns: Optional[Sequence[str]] = None,
        filters: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "pyarrow.Table":
        entry = self._parquet_entry(name)
        return self._query().read(
            self.repository,
            self.stats_ref,
            entry.data_path(),
            columns=columns,
            filters=filters,
            limit=limit,
        )

    def sql(self, name: str, sql: str) -> "pyarrow.Table":
        """Run arbitrary DuckDB SQL against a single named stat.

        Reference the stat in ``sql`` with the placeholder ``{t}``.
        """
        entry = self._parquet_entry(name)
        return self._query().sql(
            self.repository,
            self.stats_ref,
            sql,
            tables={"t": entry.data_path()},
        )

    def token_lengths(
        self,
        *,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> "pyarrow.Table":
        return self.read(STAT_TOKEN_LENGTHS, columns=columns, limit=limit)

    def duplicates(
        self,
        *,
        min_count: int = 2,
        limit: Optional[int] = None,
    ) -> "pyarrow.Table":
        return self.read(
            STAT_DUPLICATES,
            filters=f"count >= {int(min_count)}",
            limit=limit,
        )

    def pii(
        self,
        *,
        finding_types: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> "pyarrow.Table":
        filters = None
        if finding_types:
            quoted = ", ".join(_sql_string_literal(t) for t in finding_types)
            filters = f"finding_type IN ({quoted})"
        return self.read(STAT_PII, filters=filters, limit=limit)

    def _parquet_entry(self, name: str) -> StatEntry:
        entry = self.entry(name)
        if entry.data_format is not DataFormat.PARQUET:
            raise DatasetStatsError(
                f"stat {name!r} has data_format={entry.data_format.value!r}; "
                f"use summary() for JSON stats."
            )
        return entry

    def _load_manifest(self) -> StatsManifest:
        try:
            raw = self._objects.get_object(
                repository=self.repository,
                ref=self.stats_ref,
                path=MANIFEST_FILENAME,
            )
        except NotFoundException as exc:
            raise DatasetStatsError(
                f"no stats manifest at {self.repository}@{self.stats_ref}:"
                f"{MANIFEST_FILENAME}"
            ) from exc
        try:
            return StatsManifest.from_json(bytes(raw).decode("utf-8"))
        except ManifestError as exc:
            raise DatasetStatsError(f"invalid manifest: {exc}") from exc

    def _current_commit(self, *, reload: bool = False) -> str:
        if self._current_commit_cache is None or reload:
            result = self._refs.log_commits(
                repository=self.repository, ref=self.ref, amount=1, limit=True,
            )
            if not result.results:
                raise DatasetStatsError(
                    f"could not resolve current commit for "
                    f"{self.repository}@{self.ref}"
                )
            self._current_commit_cache = result.results[0].id
        return self._current_commit_cache

    def _query(self):
        if self._parquet_query_cache is None:
            from surogate_hub_sdk.parquet import ParquetQuery

            self._parquet_query_cache = ParquetQuery(self._api_client)
        return self._parquet_query_cache


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
