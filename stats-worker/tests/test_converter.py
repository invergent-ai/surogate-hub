"""End-to-end tests for DatasetConverter — both save_to_disk and raw source."""

from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from surogate_hub_worker.config import Config
from surogate_hub_worker.converter import (
    DatasetConverter,
    _discover_source_layout,
    _split_for_filename,
)
from tests.fakes import InMemoryObjectStore


def _config():
    return Config(
        hub_url="http://hub/api/v1",
        hub_access_key="AK",
        hub_secret_key="SK",
    )


def _install_fs(monkeypatch, store: InMemoryObjectStore):
    """Replace any SurogateHubFileSystem created during the test with one
    whose SDK is our in-memory Store. fsspec caches instances by kwargs
    across the process — clear it so each test gets a fresh FS."""
    from surogate_hub_worker.fs import SurogateHubFileSystem

    SurogateHubFileSystem._cache.clear()

    def _patched(self, host=None, username=None, password=None, *, api_client=None, **kw):
        from fsspec.spec import AbstractFileSystem
        AbstractFileSystem.__init__(self, **kw)
        self._api_client = None
        self._objects = store
        self._repos = store
        self._list_page_size = 1000

    monkeypatch.setattr(SurogateHubFileSystem, "__init__", _patched, raising=True)


def test_split_for_filename_recognizes_hf_patterns():
    assert _split_for_filename("train.jsonl") == "train"
    assert _split_for_filename("train-00000-of-00005.parquet") == "train"
    assert _split_for_filename("test.csv") == "test"
    assert _split_for_filename("validation.json") == "validation"
    assert _split_for_filename("eval.jsonl") == "eval"
    assert _split_for_filename("train-extra.parquet") == "train"
    assert _split_for_filename("trigger.txt") is None
    assert _split_for_filename("data.parquet") is None  # no split prefix


def test_discover_layout_root_files_and_split_dirs(monkeypatch):
    store = InMemoryObjectStore({
        "train.jsonl": b"",
        "test-00000.parquet": b"",
        "validation/shard-0.jsonl": b"",
        "validation/shard-1.jsonl": b"",
        "README.md": b"",
    })
    _install_fs(monkeypatch, store)
    import fsspec
    fs = fsspec.filesystem("shub")
    layout = _discover_source_layout(fs, "shub://repo/main")
    assert set(layout) == {"train", "test", "validation"}
    assert layout["train"] == ["shub://repo/main/train.jsonl"]
    assert layout["test"] == ["shub://repo/main/test-00000.parquet"]
    assert layout["validation"] == [
        "shub://repo/main/validation/shard-0.jsonl",
        "shub://repo/main/validation/shard-1.jsonl",
    ]


def test_convert_source_jsonl(monkeypatch):
    rows = [{"id": i, "text": f"hello {i}"} for i in range(5)]
    jsonl = ("\n".join(json.dumps(r) for r in rows) + "\n").encode()
    store = InMemoryObjectStore({"train.jsonl": jsonl})
    _install_fs(monkeypatch, store)

    result = DatasetConverter(_config()).convert("repo", "main")
    assert result.error is None
    assert len(result.splits) == 1
    split = result.splits[0]
    assert split.split_name == "train"
    assert split.row_count == 5
    assert split.schema is not None
    assert set(split.schema.names) == {"id", "text"}
    back = pq.read_table(io.BytesIO(split.content))
    assert back.num_rows == 5
    assert back.column("text").to_pylist() == [f"hello {i}" for i in range(5)]


def test_convert_source_parquet_passthrough(monkeypatch):
    tbl = pa.table({"x": [1, 2, 3]})
    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    store = InMemoryObjectStore({"train.parquet": buf.getvalue()})
    _install_fs(monkeypatch, store)

    result = DatasetConverter(_config()).convert("repo", "main")
    assert result.error is None
    assert result.splits[0].row_count == 3


def test_convert_source_csv(monkeypatch):
    csv_bytes = b"a,b\n1,x\n2,y\n3,z\n"
    store = InMemoryObjectStore({"train.csv": csv_bytes})
    _install_fs(monkeypatch, store)

    result = DatasetConverter(_config()).convert("repo", "main")
    assert result.error is None
    assert result.splits[0].row_count == 3
    assert set(result.splits[0].schema.names) == {"a", "b"}


def test_convert_save_to_disk_layout(monkeypatch):
    tbl = pa.table({"Text": ["a", "b", "c"]})
    # Arrow IPC stream format (what HF save_to_disk writes).
    sink = io.BytesIO()
    with ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    store = InMemoryObjectStore({
        "dataset_dict.json": json.dumps({"splits": ["train"]}).encode(),
        "train/data-00000-of-00001.arrow": sink.getvalue(),
        "train/dataset_info.json": b"{}",
    })
    _install_fs(monkeypatch, store)

    result = DatasetConverter(_config()).convert("repo", "main")
    assert result.error is None
    assert {s.split_name for s in result.splits} == {"train"}
    assert result.splits[0].row_count == 3


def test_convert_returns_error_on_unknown_layout(monkeypatch):
    store = InMemoryObjectStore({"README.md": b"just docs", "random.txt": b"nope"})
    _install_fs(monkeypatch, store)

    result = DatasetConverter(_config()).convert("repo", "main")
    assert result.error is not None
    assert result.splits == []
