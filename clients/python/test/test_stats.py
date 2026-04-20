"""Tests for surogate_hub_sdk.stats.

ObjectsApi and RefsApi are mocked; parquet stats point at real local files
so DuckDB's httpfs runs end-to-end through ParquetQuery.
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

try:
    import pyarrow as pa
    import pyarrow.parquet as pq_writer
except ImportError:  # pragma: no cover
    pa = None

from surogate_hub_sdk.models.commit import Commit
from surogate_hub_sdk.models.commit_list import CommitList
from surogate_hub_sdk.models.object_stats import ObjectStats
from surogate_hub_sdk.models.pagination import Pagination


_NOW = 1_700_000_000


def _make_commit(cid: str) -> Commit:
    return Commit(
        id=cid,
        parents=[],
        committer="test",
        message="x",
        metadata={},
        creation_date=_NOW,
        meta_range_id="",
    )


def _make_commit_list(cid: str) -> CommitList:
    return CommitList(
        pagination=Pagination(
            has_more=False, next_offset="", results=1, max_per_page=1,
        ),
        results=[_make_commit(cid)],
    )


@unittest.skipIf(pa is None, "pyarrow not installed")
class DatasetStatsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

        dup_table = pa.table({
            "content_hash": ["a", "b", "c"],
            "count": [5, 2, 1],
        })
        pii_table = pa.table({
            "path": ["f1", "f2", "f3"],
            "finding_type": ["email", "ssn", "email"],
            "span": [[0, 10], [5, 14], [20, 30]],
        })
        tok_table = pa.table({
            "path": ["f1", "f2"],
            "row_idx": [0, 1],
            "token_count": [128, 4096],
        })
        self.dup_path = os.path.join(self.tmp, "dup.parquet")
        self.pii_path = os.path.join(self.tmp, "pii.parquet")
        self.tok_path = os.path.join(self.tmp, "tok.parquet")
        pq_writer.write_table(dup_table, self.dup_path)
        pq_writer.write_table(pii_table, self.pii_path)
        pq_writer.write_table(tok_table, self.tok_path)

        self.manifest = {
            "manifest_version": 1,
            "stats": {
                "duplicates": {
                    "version": "v1",
                    "source_commit": "HEAD_COMMIT",
                    "produced_at": "2026-04-20T12:00:00Z",
                    "producer": "shub-stats@0.1",
                    "row_count": 3,
                },
                "pii": {
                    "version": "v1",
                    "source_commit": "STALE_COMMIT",
                    "produced_at": "2026-03-01T00:00:00Z",
                    "producer": "shub-stats@0.1",
                },
                "token_lengths": {
                    "version": "v1",
                    "source_commit": "HEAD_COMMIT",
                    "produced_at": "2026-04-20T12:00:00Z",
                    "producer": "shub-stats@0.1",
                    "parameters": {"tokenizer": "cl100k_base"},
                },
                "summary": {
                    "version": "v1",
                    "source_commit": "HEAD_COMMIT",
                    "produced_at": "2026-04-20T12:00:00Z",
                    "producer": "shub-stats@0.1",
                    "data_format": "json",
                },
            },
        }
        self.summary = {"row_count": 14238119, "bytes": 107374182400}

    def _make_stats(self, manifest_bytes=None):
        from surogate_hub_sdk.stats import DatasetStats

        stats = DatasetStats.__new__(DatasetStats)
        stats._api_client = MagicMock()
        stats._objects = self._mock_objects_api(manifest_bytes)
        stats._refs = self._mock_refs_api()
        stats.repository = "repo"
        stats.ref = "main"
        stats.stats_ref = "_stats_main"
        stats._manifest = None
        stats._current_commit_cache = None
        stats._parquet_query_cache = self._make_parquet_query(stats._objects)
        return stats

    def _make_parquet_query(self, objects_api):
        from surogate_hub_sdk.parquet import ParquetQuery

        pq = ParquetQuery.__new__(ParquetQuery)
        pq._api_client = MagicMock()
        pq._objects = objects_api
        pq._list_page_size = 1000
        pq._duckdb_threads = None
        pq._presign_supported = None
        return pq

    def _mock_objects_api(self, manifest_bytes=None):
        api = MagicMock()
        parquet_by_stat = {
            "duplicates/v1/data.parquet": self.dup_path,
            "pii/v1/data.parquet": self.pii_path,
            "token_lengths/v1/data.parquet": self.tok_path,
        }
        manifest_payload = (
            manifest_bytes
            if manifest_bytes is not None
            else json.dumps(self.manifest).encode()
        )

        def _get_object(repository, ref, path):
            if ref != "_stats_main":
                raise AssertionError(
                    f"stats reads should hit stats_ref, got ref={ref!r}"
                )
            if path == "manifest.json":
                return manifest_payload
            if path == "summary/v1/data.json":
                return json.dumps(self.summary).encode()
            raise AssertionError(f"unexpected get_object path: {path}")

        api.get_object.side_effect = _get_object

        def _stats(path):
            return ObjectStats(
                path=path,
                path_type="object",
                physical_address=f"file://{parquet_by_stat[path]}",
                checksum="x", size_bytes=1, mtime=0,
            )

        def _stat_object(repository, ref, path, presign):
            if ref != "_stats_main":
                raise AssertionError(
                    f"stats reads should hit stats_ref, got ref={ref!r}"
                )
            return _stats(path)

        api.stat_object.side_effect = _stat_object
        return api

    def _mock_refs_api(self):
        api = MagicMock()

        def _log(repository, ref, amount, limit):
            if ref != "main":
                raise AssertionError(
                    f"freshness should resolve data ref, got ref={ref!r}"
                )
            return _make_commit_list("HEAD_COMMIT")

        api.log_commits.side_effect = _log
        return api

    def test_list_returns_manifest_names(self):
        stats = self._make_stats()
        self.assertEqual(
            stats.list(),
            ["duplicates", "pii", "summary", "token_lengths"],
        )

    def test_entry_unknown_raises(self):
        from surogate_hub_sdk.stats import DatasetStatsError

        stats = self._make_stats()
        with self.assertRaises(DatasetStatsError):
            stats.entry("missing")

    def test_freshness_fresh_stale_and_missing(self):
        from surogate_hub_sdk.stats import Freshness

        stats = self._make_stats()
        self.assertIs(stats.freshness("duplicates").state, Freshness.FRESH)
        self.assertIs(stats.freshness("pii").state, Freshness.STALE)
        self.assertIs(stats.freshness("does_not_exist").state, Freshness.MISSING)
        self.assertTrue(stats.freshness("duplicates").is_fresh)
        self.assertFalse(stats.freshness("pii").is_fresh)

    def test_summary_reads_json(self):
        stats = self._make_stats()
        self.assertEqual(stats.summary(), self.summary)

    def test_summary_rejects_parquet_stat(self):
        from surogate_hub_sdk.stats import DatasetStatsError

        stats = self._make_stats()
        with self.assertRaises(DatasetStatsError):
            stats.summary("duplicates")

    def test_duplicates_applies_min_count_filter(self):
        stats = self._make_stats()
        table = stats.duplicates(min_count=2)
        hashes = set(table.column("content_hash").to_pylist())
        self.assertEqual(hashes, {"a", "b"})

    def test_pii_filters_by_finding_type(self):
        stats = self._make_stats()
        table = stats.pii(finding_types=["email"])
        self.assertEqual(set(table.column("finding_type").to_pylist()), {"email"})
        self.assertEqual(table.num_rows, 2)

    def test_pii_escapes_single_quotes_safely(self):
        stats = self._make_stats()
        table = stats.pii(finding_types=["email'; DROP TABLE x--"])
        self.assertEqual(table.num_rows, 0)

    def test_token_lengths_projection(self):
        stats = self._make_stats()
        table = stats.token_lengths(columns=["token_count"])
        self.assertEqual(table.column_names, ["token_count"])
        self.assertEqual(sorted(table.column("token_count").to_pylist()), [128, 4096])

    def test_read_rejects_json_stat(self):
        from surogate_hub_sdk.stats import DatasetStatsError

        stats = self._make_stats()
        with self.assertRaises(DatasetStatsError):
            stats.read("summary")

    def test_sql_placeholder_substitution(self):
        stats = self._make_stats()
        table = stats.sql(
            "duplicates",
            "SELECT content_hash FROM {t} WHERE count >= 2 ORDER BY content_hash",
        )
        self.assertEqual(table.column("content_hash").to_pylist(), ["a", "b"])

    def test_missing_manifest_raises(self):
        from surogate_hub_sdk.exceptions import NotFoundException
        from surogate_hub_sdk.stats import DatasetStatsError

        stats = self._make_stats()

        def _raise(repository, ref, path):
            raise NotFoundException(status=404, reason="Not Found")

        stats._objects.get_object.side_effect = _raise
        with self.assertRaises(DatasetStatsError):
            stats.manifest()

    def test_malformed_manifest_raises(self):
        from surogate_hub_sdk.stats import DatasetStatsError

        stats = self._make_stats(manifest_bytes=b"not json at all")
        with self.assertRaises(DatasetStatsError):
            stats.manifest()


if __name__ == "__main__":
    unittest.main()
