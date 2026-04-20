"""Convert a Surogate Hub dataset repo/ref to parquet via our ``shub://`` fsspec FS.

Two input layouts are supported, both streamed (no local staging of the full
dataset so this scales to 100GB+):

* **``save_to_disk`` layout** (``dataset_dict.json`` / ``dataset_info.json``
  present, data in ``.arrow`` files): read the arrow IPC streams directly
  with :mod:`pyarrow.ipc`.
* **Raw source files** (no HF metadata, just ``train.jsonl`` /
  ``train/*.parquet`` / etc.): discover splits from HF filename conventions
  and stream each file via :mod:`pyarrow` readers (``pyarrow.json`` /
  ``pyarrow.parquet`` / ``pyarrow.csv``).

Output per split is a single parquet blob plus an HF-compatible
``dataset_info.json``; the writer additionally emits a root
``dataset_dict.json`` so the parquet branch is a self-describing HF dataset.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import fsspec
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.json as pajson
import pyarrow.parquet as pq

import surogate_hub_worker  # noqa: F401 — registers 'shub://' protocol

from surogate_hub_worker.config import Config
from surogate_hub_worker.fs import PROTOCOL, PROTOCOL_SCHEME


log = logging.getLogger(__name__)


_SPLIT_NAMES = frozenset({"train", "test", "validation", "val", "eval", "dev"})
_SUPPORTED_EXTS = frozenset({"parquet", "jsonl", "json", "csv"})
# HF save_to_disk metadata files we should never treat as data shards.
_HF_METADATA_FILES = frozenset({"dataset_info.json", "state.json", "dataset_dict.json"})
_BATCH_SIZE = 10_000


@dataclass
class ConvertedSplit:
    config_name: str
    split_name: str
    content: bytes
    row_count: int
    schema: Optional[pa.Schema] = None
    num_bytes: int = 0

    def path(self) -> str:
        # Single-config datasets use the flat ``<split>/data.parquet`` layout
        # so the parquet branch is directly loadable by HF ``load_from_disk``.
        return f"{self._prefix()}data.parquet"

    def info_path(self) -> str:
        return f"{self._prefix()}dataset_info.json"

    def dataset_info_json(self, repository: str) -> bytes:
        """Serialize this split as HF ``dataset_info.json`` bytes."""
        payload = {
            "dataset_name": repository,
            "config_name": self.config_name,
            "features": _schema_to_hf_features(self.schema),
            "splits": {
                self.split_name: {
                    "name": self.split_name,
                    "num_examples": self.row_count,
                    "num_bytes": self.num_bytes,
                },
            },
        }
        return json.dumps(payload, indent=2).encode("utf-8")

    def _prefix(self) -> str:
        if self.config_name == "default":
            return f"{self.split_name}/"
        return f"{self.config_name}/{self.split_name}/"


@dataclass
class ConversionResult:
    splits: List[ConvertedSplit] = field(default_factory=list)
    configs_seen: List[str] = field(default_factory=list)
    error: Optional[str] = None


class DatasetConverter:
    def __init__(self, config: Config) -> None:
        self._config = config

    def convert(self, repository: str, data_ref: str) -> ConversionResult:
        storage_options = {
            "host": self._config.hub_url,
            "username": self._config.hub_access_key,
            "password": self._config.hub_secret_key,
        }
        base_path = f"{PROTOCOL_SCHEME}{repository}/{data_ref}"
        fs = fsspec.filesystem(PROTOCOL, **storage_options)

        saved_to_disk = any(
            fs.exists(f"{base_path}/{marker}")
            for marker in ("dataset_dict.json", "dataset_info.json")
        )
        if saved_to_disk:
            return self._convert_saved_to_disk(base_path, fs)
        return self._convert_source_files(base_path, fs)

    def _convert_saved_to_disk(self, base_path: str, fs) -> ConversionResult:
        splits = self._discover_saved_to_disk_splits(base_path, fs)
        if not splits:
            return ConversionResult(error="no splits detected in save_to_disk layout")

        result = ConversionResult(configs_seen=["default"])
        for split_name, shard_paths in splits.items():
            if not shard_paths:
                log.warning(
                    "no shards found for split %s under save_to_disk layout — skipping",
                    split_name,
                )
                if result.error is None:
                    result.error = f"no shards for split {split_name!r}"
                continue
            try:
                content, rows, schema = _shards_to_parquet(fs, shard_paths)
            except Exception as exc:
                log.warning(
                    "shard->parquet failed for split %s: %s",
                    split_name, exc, exc_info=True,
                )
                if result.error is None:
                    result.error = f"shard->parquet {split_name!r}: {exc}"
                continue
            result.splits.append(ConvertedSplit(
                config_name="default", split_name=split_name,
                content=content, row_count=rows,
                schema=schema, num_bytes=len(content),
            ))
            log.info(
                "converted (save_to_disk) default:%s (%d rows, %d bytes, %d shard(s))",
                split_name, rows, len(content), len(shard_paths),
            )
        return result

    def _convert_source_files(self, base_path: str, fs) -> ConversionResult:
        layout = _discover_source_layout(fs, base_path)
        if not layout:
            return ConversionResult(
                error="no files matching HF split conventions (train/test/validation)"
            )

        result = ConversionResult(configs_seen=["default"])
        for split_name, urls in layout.items():
            try:
                content, rows, schema = _stream_urls_to_parquet(fs, urls)
            except Exception as exc:
                log.warning(
                    "source->parquet failed for split %s: %s",
                    split_name, exc, exc_info=True,
                )
                if result.error is None:
                    result.error = f"source->parquet {split_name!r}: {exc}"
                continue
            result.splits.append(ConvertedSplit(
                config_name="default", split_name=split_name,
                content=content, row_count=rows,
                schema=schema, num_bytes=len(content),
            ))
            log.info(
                "converted (source) default:%s (%d rows, %d bytes, %d file(s))",
                split_name, rows, len(content), len(urls),
            )
        return result

    def _discover_saved_to_disk_splits(self, base_path: str, fs) -> Dict[str, List[str]]:
        dict_path = f"{base_path}/dataset_dict.json"
        if fs.exists(dict_path):
            with fs.open(dict_path, "rb") as fh:
                payload = json.load(fh)
            split_names = payload.get("splits") or []
            return {name: self._split_shards(base_path, fs, name) for name in split_names}
        if fs.exists(f"{base_path}/dataset_info.json"):
            return {"train": self._split_shards(base_path, fs, "")}
        return {}

    def _split_shards(self, base_path: str, fs, split_name: str) -> List[str]:
        """Per-split shard URLs in a save_to_disk layout.

        Accepts arrow IPC shards (the HF default) or any
        ``_SUPPORTED_EXTS`` format (parquet/jsonl/csv) so splits added
        outside HF's ``save_to_disk`` (e.g. a synthetic pipeline run
        dropping a single parquet) convert to the mirror cleanly.
        """
        split_dir = f"{base_path}/{split_name}" if split_name else base_path
        urls: List[str] = []
        for entry in fs.ls(split_dir, detail=True):
            if entry["type"] != "file":
                continue
            name = entry["name"].rsplit("/", 1)[-1]
            if name in _HF_METADATA_FILES:
                continue
            if name.endswith(".arrow") or _extension_of(name) in _SUPPORTED_EXTS:
                urls.append(_as_shub_url(entry["name"]))
        return sorted(urls)


def _discover_source_layout(fs, base_path: str) -> Dict[str, List[str]]:
    """Map split name → URLs following HF conventions.

    Matches ``train.jsonl``, ``train-00000-of-00003.parquet``, and
    ``train/*.{parquet,jsonl,json,csv}`` (same for ``test`` / ``validation``
    / ``val`` / ``eval`` / ``dev``).
    """
    splits: Dict[str, List[str]] = {}
    for entry in fs.ls(base_path, detail=True):
        name = entry["name"].rsplit("/", 1)[-1]
        if entry["type"] == "file":
            split = _split_for_filename(name)
            if split is not None:
                splits.setdefault(split, []).append(_as_shub_url(entry["name"]))
        elif entry["type"] == "directory" and name in _SPLIT_NAMES:
            for sub in fs.ls(entry["name"], detail=True):
                if sub["type"] != "file":
                    continue
                if _extension_of(sub["name"].rsplit("/", 1)[-1]) in _SUPPORTED_EXTS:
                    splits.setdefault(name, []).append(_as_shub_url(sub["name"]))
    for urls in splits.values():
        urls.sort()
    return splits


def _split_for_filename(name: str) -> Optional[str]:
    ext = _extension_of(name)
    if ext not in _SUPPORTED_EXTS:
        return None
    stem = name[: -(len(ext) + 1)]  # drop ".<ext>"
    leading = stem.split("-", 1)[0]
    return leading if leading in _SPLIT_NAMES else None


def _extension_of(filename: str) -> str:
    _, _, ext = filename.rpartition(".")
    return ext.lower()


def _as_shub_url(ls_name: str) -> str:
    return f"{PROTOCOL_SCHEME}{ls_name.lstrip('/')}"


def _stream_urls_to_parquet(
    fs, urls: List[str],
) -> Tuple[bytes, int, pa.Schema]:
    return _batches_to_parquet(_iter_urls_batches(fs, urls))


def _shards_to_parquet(
    fs, shard_paths: List[str],
) -> Tuple[bytes, int, pa.Schema]:
    """save_to_disk shard → parquet, dispatching on the per-shard extension."""
    return _batches_to_parquet(_iter_shard_batches(fs, shard_paths))


def _iter_shard_batches(fs, shard_paths: List[str]):
    """Yield batches from arrow/parquet/jsonl/csv shards.

    HF ``save_to_disk`` emits arrow IPC shards; pipelines that add a
    split out-of-band (e.g. a synthetic run writing a parquet) mix in
    non-arrow shards.  Dispatch per-shard instead of assuming a single
    format across the split.
    """
    for shard in shard_paths:
        url = shard if "://" in shard else _as_shub_url(shard)
        ext = _extension_of(url)
        if ext == "arrow" or ext == "":
            with fs.open(url, "rb") as fh:
                reader = _open_arrow_reader(fh)
                for batch in reader:
                    yield batch
        else:
            yield from _iter_urls_batches(fs, [url])


def _batches_to_parquet(batches) -> Tuple[bytes, int, pa.Schema]:
    """Consume a RecordBatch iterator into one zstd parquet blob."""
    buf = BytesIO()
    writer: Optional[pq.ParquetWriter] = None
    rows = 0
    schema: Optional[pa.Schema] = None
    try:
        for batch in batches:
            if writer is None:
                schema = batch.schema
                writer = pq.ParquetWriter(buf, schema, compression="zstd")
            writer.write_batch(batch)
            rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
    return buf.getvalue(), rows, schema or pa.schema([])


def _iter_urls_batches(fs, urls: List[str]):
    for url in urls:
        ext = _extension_of(url)
        with fs.open(url, "rb") as fh:
            if ext == "parquet":
                yield from pq.ParquetFile(fh).iter_batches(batch_size=_BATCH_SIZE)
            elif ext in {"jsonl", "json"}:
                table = pajson.read_json(fh)
                yield from table.to_batches(max_chunksize=_BATCH_SIZE)
            elif ext == "csv":
                reader = pacsv.open_csv(fh)
                while True:
                    try:
                        yield reader.read_next_batch()
                    except StopIteration:
                        break
            else:
                raise ValueError(f"unsupported extension: {ext!r}")


def _open_arrow_reader(fh):
    try:
        return ipc.open_stream(fh)
    except pa.ArrowInvalid:
        fh.seek(0)
        return ipc.open_file(fh)


def _schema_to_hf_features(schema) -> Dict[str, Any]:
    """Project a pyarrow schema into HF ``Features`` dict form.

    Uses ``datasets.Features.from_arrow_schema`` so nested/array types
    round-trip correctly; falls back to a plain type-name map if the
    library or schema is missing.
    """
    if schema is None or len(schema) == 0:
        return {}
    try:
        from datasets import Features

        return Features.from_arrow_schema(schema).to_dict()
    except Exception:  # pragma: no cover
        return {field.name: str(field.type) for field in schema}
