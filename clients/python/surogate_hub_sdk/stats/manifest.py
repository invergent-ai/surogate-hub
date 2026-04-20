"""Manifest schema for the ``_stats_<data_ref>`` sidecar convention.

Hand-written module (not generated). See ``.openapi-generator-ignore``.

The manifest is authoritative: readers should never glob for stat files.
If a stat isn't listed here it doesn't exist from the worker's point of view.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


MANIFEST_FILENAME = "manifest.json"
DATA_FILE_STEM = "data"
_SUPPORTED_MANIFEST_VERSIONS = {1}


class DataFormat(str, enum.Enum):
    PARQUET = "parquet"
    JSON = "json"


class ManifestError(Exception):
    pass


@dataclass
class StatEntry:
    name: str
    version: str
    source_commit: str
    produced_at: str
    producer: str
    row_count: Optional[int] = None
    data_format: DataFormat = DataFormat.PARQUET
    parameters: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, name: str, raw: Dict[str, Any]) -> "StatEntry":
        required = ("version", "source_commit", "produced_at", "producer")
        missing = [k for k in required if k not in raw]
        if missing:
            raise ManifestError(
                f"stat {name!r} missing required keys: {missing}"
            )
        fmt_raw = raw.get("data_format", DataFormat.PARQUET.value)
        try:
            data_format = DataFormat(fmt_raw)
        except ValueError as exc:
            raise ManifestError(
                f"stat {name!r} has unsupported data_format={fmt_raw!r}; "
                f"expected one of {[f.value for f in DataFormat]}"
            ) from exc
        return cls(
            name=name,
            version=raw["version"],
            source_commit=raw["source_commit"],
            produced_at=raw["produced_at"],
            producer=raw["producer"],
            row_count=raw.get("row_count"),
            data_format=data_format,
            parameters=raw.get("parameters", {}) or {},
        )

    def data_path(self) -> str:
        return f"{self.name}/{self.version}/{DATA_FILE_STEM}.{self.data_format.value}"

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "version": self.version,
            "source_commit": self.source_commit,
            "produced_at": self.produced_at,
            "producer": self.producer,
            "data_format": self.data_format.value,
        }
        if self.row_count is not None:
            out["row_count"] = self.row_count
        if self.parameters:
            out["parameters"] = dict(self.parameters)
        return out


@dataclass
class StatsManifest:
    stats: Dict[str, StatEntry]
    manifest_version: int = 1

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "StatsManifest":
        version = raw.get("manifest_version", 1)
        if version not in _SUPPORTED_MANIFEST_VERSIONS:
            raise ManifestError(
                f"unsupported manifest_version {version}; "
                f"this SDK supports {sorted(_SUPPORTED_MANIFEST_VERSIONS)}"
            )
        stats_raw = raw.get("stats") or {}
        if not isinstance(stats_raw, dict):
            raise ManifestError("manifest 'stats' must be an object")
        return cls(
            manifest_version=version,
            stats={
                name: StatEntry.from_dict(name, entry)
                for name, entry in stats_raw.items()
            },
        )

    @classmethod
    def from_json(cls, text: str) -> "StatsManifest":
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise ManifestError("manifest root must be a JSON object")
        return cls.from_dict(raw)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "stats": {
                name: entry.to_dict() for name, entry in self.stats.items()
            },
        }

    def to_json(self, *, indent: Optional[int] = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)
