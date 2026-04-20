"""Stat computers.

Each computer takes a DuckDB connection prepared by :func:`register_view`
(which loads ``httpfs`` and registers the dataset's parquet URLs as a view
named :data:`_VIEW_NAME`) and returns an :class:`Artifact` ready to be
uploaded by the stats writer.

v1 ships without a tokenizer: ``token_lengths`` uses the industry-standard
``length(text) // 4`` char-count approximation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from surogate_hub_sdk.stats.manifest import DataFormat


_VIEW_NAME = "t"


@dataclass
class Artifact:
    stat_name: str
    version: str
    data_format: DataFormat
    content: bytes
    row_count: int | None = None
    parameters: Dict[str, Any] = field(default_factory=dict)


def register_view(con: duckdb.DuckDBPyConnection, urls: List[str]) -> None:
    # Container users (e.g. the Debian 'lakefs' account in our image) have
    # ``/nonexistent`` as HOME; DuckDB needs a writable directory to cache
    # extensions. ``/tmp`` is always writable in a pod.
    con.execute("SET home_directory='/tmp'")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # ``union_by_name=true`` fills missing columns with NULL when splits on
    # the same parquet branch diverge in schema (e.g. an HF dataset where a
    # pipeline injects a ``synthetic`` split alongside ``train`` / ``test``).
    # Use the SQL form, not Python ``from_parquet(urls, union_by_name=True)``
    # — the latter hangs in DuckDB 1.5.2 on multi-file fsspec-routed reads.
    quoted = ", ".join("'" + u.replace("'", "''") + "'" for u in urls)
    con.execute(
        f"CREATE OR REPLACE VIEW {_VIEW_NAME} AS "
        f"SELECT * FROM read_parquet([{quoted}], union_by_name=true)"
    )


def _ensure_table(arrow_result) -> pa.Table:
    return arrow_result if isinstance(arrow_result, pa.Table) else arrow_result.read_all()


def _string_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
    desc = con.execute(f"DESCRIBE SELECT * FROM {_VIEW_NAME}").fetchall()
    return [row[0] for row in desc if str(row[1]).upper().startswith("VARCHAR")]


def _parquet_bytes(table: pa.Table) -> bytes:
    buf = BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def compute_summary(con: duckdb.DuckDBPyConnection) -> Artifact:
    totals = con.execute(
        f"SELECT COUNT(*) AS rows FROM {_VIEW_NAME}"
    ).fetchone()
    total_rows = int(totals[0]) if totals else 0

    per_column: Dict[str, Dict[str, Any]] = {}
    for col in _string_columns(con):
        row = con.execute(
            f"""
            SELECT
                approx_quantile(length({col}), 0.5)  AS p50,
                approx_quantile(length({col}), 0.95) AS p95,
                approx_quantile(length({col}), 0.99) AS p99,
                AVG(length({col}))                   AS mean,
                MAX(length({col}))                   AS max
            FROM {_VIEW_NAME}
            WHERE {col} IS NOT NULL
            """
        ).fetchone()
        if row is None:
            continue
        per_column[col] = {
            "char_length_p50": row[0],
            "char_length_p95": row[1],
            "char_length_p99": row[2],
            "char_length_mean": row[3],
            "char_length_max": row[4],
        }

    payload = {"row_count": total_rows, "columns": per_column}
    return Artifact(
        stat_name="summary",
        version="v1",
        data_format=DataFormat.JSON,
        content=json.dumps(payload).encode("utf-8"),
        row_count=total_rows,
    )


def compute_duplicates(con: duckdb.DuckDBPyConnection) -> Artifact:
    # hash(t) is a native DuckDB row hash over the whole struct — much
    # cheaper than md5(to_json(t)) because it avoids materializing a JSON
    # string per row.
    table = _ensure_table(con.execute(
        f"""
        SELECT
            hash({_VIEW_NAME}) AS content_hash,
            COUNT(*)           AS count
        FROM {_VIEW_NAME}
        GROUP BY content_hash
        HAVING count > 1
        ORDER BY count DESC
        """
    ).arrow())
    return Artifact(
        stat_name="duplicates",
        version="v1",
        data_format=DataFormat.PARQUET,
        content=_parquet_bytes(table),
        row_count=table.num_rows,
        parameters={"scope": "full_row", "hash": "duckdb_native_64bit"},
    )


_PII_PATTERNS: Dict[str, str] = {
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    "phone_us": r"(?:\+?1[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}",
    "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
    "credit_card": r"\b(?:\d[ -]*?){13,16}\b",
    "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
}


def compute_pii(
    con: duckdb.DuckDBPyConnection, *, text_column: str,
) -> Artifact:
    _validate_column(text_column)
    # Cross-join the dataset against an inline patterns table so we scan
    # the text column once instead of N_patterns times via UNION ALL.
    pattern_rows = ", ".join(
        f"('{name}', '{pat.replace(chr(39), chr(39) * 2)}')"
        for name, pat in _PII_PATTERNS.items()
    )
    sql = f"""
        WITH numbered AS (
            SELECT *, row_number() OVER () - 1 AS row_idx FROM {_VIEW_NAME}
        ),
        patterns(finding_type, pattern) AS (VALUES {pattern_rows})
        SELECT
            p.finding_type,
            n.{text_column} AS match_text,
            n.row_idx
        FROM numbered n, patterns p
        WHERE regexp_matches(n.{text_column}, p.pattern)
        ORDER BY p.finding_type, n.row_idx
    """
    table = _ensure_table(con.execute(sql).arrow())
    return Artifact(
        stat_name="pii",
        version="v1",
        data_format=DataFormat.PARQUET,
        content=_parquet_bytes(table),
        row_count=table.num_rows,
        parameters={
            "text_column": text_column,
            "patterns": list(_PII_PATTERNS.keys()),
            "method": "regex",
        },
    )


def compute_token_lengths(
    con: duckdb.DuckDBPyConnection, *, text_column: str,
) -> Artifact:
    _validate_column(text_column)
    table = _ensure_table(con.execute(
        f"""
        SELECT
            row_number() OVER () - 1 AS row_idx,
            length({text_column}) AS char_count,
            CAST(FLOOR(length({text_column}) / 4.0) AS INTEGER) AS token_count_approx
        FROM {_VIEW_NAME}
        WHERE {text_column} IS NOT NULL
        """
    ).arrow())
    return Artifact(
        stat_name="token_lengths",
        version="v1",
        data_format=DataFormat.PARQUET,
        content=_parquet_bytes(table),
        row_count=table.num_rows,
        parameters={
            "text_column": text_column,
            "method": "char_count_div_4_approx",
        },
    )


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_column(name: str) -> None:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"unsafe column name: {name!r}")
