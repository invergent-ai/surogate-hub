"""Stat computers are the heart of the worker; test against real DuckDB."""

from __future__ import annotations

import json
from io import BytesIO

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from surogate_hub_sdk.stats.manifest import DataFormat
from surogate_hub_worker import computers


def _connect(urls):
    con = duckdb.connect(database=":memory:")
    computers.register_view(con, urls)
    return con


def _read_parquet_bytes(data: bytes) -> pa.Table:
    return pq.read_table(BytesIO(data))


def test_summary_reports_row_count_and_percentiles(sample_parquet):
    con = _connect([sample_parquet])
    artifact = computers.compute_summary(con)
    assert artifact.stat_name == "summary"
    assert artifact.data_format is DataFormat.JSON
    payload = json.loads(artifact.content.decode())
    assert payload["row_count"] == 6
    assert "text" in payload["columns"]
    col_stats = payload["columns"]["text"]
    for key in ("char_length_p50", "char_length_p95", "char_length_mean", "char_length_max"):
        assert key in col_stats


def test_duplicates_finds_exact_row_dupes(sample_parquet):
    con = _connect([sample_parquet])
    artifact = computers.compute_duplicates(con)
    assert artifact.data_format is DataFormat.PARQUET
    table = _read_parquet_bytes(artifact.content)
    assert set(table.column_names) == {"content_hash", "count"}
    # only two identical rows in the fixture: (1,"hello world") and (3,"hello world")
    # different ids → actually NOT full-row duplicates under our hash scheme.
    # Confirm the property explicitly:
    assert table.num_rows == 0


def test_duplicates_detects_full_row_match(make_parquet):
    tbl = pa.table({"x": [1, 1, 2], "y": ["a", "a", "b"]})
    url = make_parquet("dup.parquet", tbl)
    con = _connect([url])
    artifact = computers.compute_duplicates(con)
    table = _read_parquet_bytes(artifact.content)
    counts = sorted(table.column("count").to_pylist())
    assert counts == [2]


def test_pii_detects_email_ssn_phone(sample_parquet):
    con = _connect([sample_parquet])
    artifact = computers.compute_pii(con, text_column="text")
    table = _read_parquet_bytes(artifact.content)
    found = sorted(set(table.column("finding_type").to_pylist()))
    assert "email" in found
    assert "ssn" in found
    assert "phone_us" in found


def test_pii_rejects_unsafe_column(sample_parquet):
    con = _connect([sample_parquet])
    with pytest.raises(ValueError):
        computers.compute_pii(con, text_column="text; DROP TABLE x")


def test_token_lengths_char_approximation(sample_parquet):
    con = _connect([sample_parquet])
    artifact = computers.compute_token_lengths(con, text_column="text")
    table = _read_parquet_bytes(artifact.content)
    assert set(table.column_names) == {"row_idx", "char_count", "token_count_approx"}
    # hello world → len 11 → 11//4 = 2
    char_to_tok = dict(zip(
        table.column("char_count").to_pylist(),
        table.column("token_count_approx").to_pylist(),
    ))
    assert char_to_tok[11] == 2
