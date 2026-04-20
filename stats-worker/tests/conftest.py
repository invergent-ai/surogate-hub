"""Shared test fixtures.

Tests write small parquet files to a temp directory and expose them as
``file://`` URLs — DuckDB's httpfs reads them identically to presigned
HTTPS URLs, so the same computation paths exercise.
"""

from __future__ import annotations

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def sample_parquet(tmp_path):
    tbl = pa.table({
        "id": [1, 2, 3, 4, 5, 6],
        "text": [
            "hello world",
            "contact me at alice@example.com",
            "hello world",
            "my ssn is 123-45-6789",
            "call 555-123-4567",
            "plain text row",
        ],
    })
    path = tmp_path / "data.parquet"
    pq.write_table(tbl, path)
    return f"file://{path}"


@pytest.fixture
def make_parquet(tmp_path):
    def _make(name, table):
        p = tmp_path / name
        pq.write_table(table, p)
        return f"file://{p}"

    return _make
