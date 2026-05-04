"""Unit tests for SurogateHubFileSystem.

The SDK is mocked via a small in-memory store so that both listing and
range reads are exercised without a live hub.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from surogate_hub_worker.fs import SurogateHubFileSystem
from tests.fakes import InMemoryObjectStore


def _make_fs(store: InMemoryObjectStore) -> SurogateHubFileSystem:
    fs = SurogateHubFileSystem(api_client=MagicMock())
    fs._objects = store
    return fs


def test_ls_dirs_and_files():
    store = InMemoryObjectStore({
        "data/train.jsonl": b"{}\n",
        "data/test.jsonl": b"{}\n",
        "data/nested/more.jsonl": b"{}\n",
        "README.md": b"# hi",
    })
    fs = _make_fs(store)
    top = {i["name"]: i["type"] for i in fs.ls("sghub://owner/repo/main/", detail=True)}
    assert top == {"/owner/repo/main/data": "directory", "/owner/repo/main/README.md": "file"}
    inner = {i["name"]: i["type"] for i in fs.ls("sghub://owner/repo/main/data", detail=True)}
    assert inner == {
        "/owner/repo/main/data/train.jsonl": "file",
        "/owner/repo/main/data/test.jsonl": "file",
        "/owner/repo/main/data/nested": "directory",
    }


def test_info_file_and_dir_and_missing():
    store = InMemoryObjectStore({"data/train.jsonl": b"hello"})
    fs = _make_fs(store)
    f = fs.info("sghub://owner/repo/main/data/train.jsonl")
    assert f["type"] == "file" and f["size"] == 5

    d = fs.info("sghub://owner/repo/main/data")
    assert d["type"] == "directory"

    with pytest.raises(FileNotFoundError):
        fs.info("sghub://owner/repo/main/does/not/exist")


def test_open_reads_bytes_with_range():
    store = InMemoryObjectStore({"blob.bin": bytes(range(256))})
    fs = _make_fs(store)
    with fs._open("sghub://owner/repo/main/blob.bin") as fh:
        assert fh.size == 256
        fh.seek(10)
        chunk = fh.read(20)
    assert chunk == bytes(range(10, 30))


def test_open_rejects_write_and_dir():
    store = InMemoryObjectStore({"a.txt": b"x"})
    fs = _make_fs(store)
    with pytest.raises(NotImplementedError):
        fs._open("sghub://owner/repo/main/a.txt", mode="wb")
    with pytest.raises(IsADirectoryError):
        fs._open("sghub://owner/repo/main/")


def test_pyarrow_parquet_reads_via_our_fs():
    """Prove the FS satisfies enough of the fsspec contract for a real
    parquet reader to use it end-to-end (it range-reads the footer, then
    the column chunks it needs)."""
    tbl = pa.table({"id": [1, 2, 3], "text": ["alpha", "beta", "gamma"]})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    store = InMemoryObjectStore({"train.parquet": buf.getvalue()})
    fs = _make_fs(store)

    with fs._open("sghub://owner/repo/main/train.parquet") as fh:
        loaded = pq.read_table(fh)
    assert loaded.num_rows == 3
    assert loaded.column("text").to_pylist() == ["alpha", "beta", "gamma"]


def test_fsspec_url_dispatch_returns_our_class():
    """datasets.load_dataset('sghub://...') will resolve the FS via
    fsspec's protocol lookup. Verify the lookup returns our class."""
    import fsspec

    assert fsspec.get_filesystem_class("sghub") is SurogateHubFileSystem
