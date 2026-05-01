"""DuckDB-backed parquet query over Surogate Hub.

Two transport modes, auto-selected per instance:

* **Presigned URLs** (preferred) — for remote blockstores (S3/GCS/Azure)
  the hub signs a storage-backend URL and DuckDB's ``httpfs`` range-reads
  it directly; the hub stays out of the hot path.
* **fsspec** (automatic fallback) — for backends that can't presign
  (``local`` in dev), we register :class:`~surogate_hub_sdk.fs.SurogateHubFileSystem`
  with the DuckDB connection and hand it ``shub://`` URLs. Every range read
  goes back through the hub API, but it Just Works on any blockstore.

The mode is detected on the first attempted ``stat_object(presign=True)`` /
``list_objects(presign=True)`` call: a 400 ``local adapter presigned URL:
operation not supported`` response flips the query to fsspec mode for the
lifetime of the instance.
"""

from __future__ import annotations

import fnmatch
import logging
import re
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Sequence

from surogate_hub_sdk.api.objects_api import ObjectsApi
from surogate_hub_sdk.api_client import ApiClient
from surogate_hub_sdk.exceptions import ApiException
from surogate_hub_sdk.fs import PROTOCOL, PROTOCOL_SCHEME

if TYPE_CHECKING:
    import pyarrow


log = logging.getLogger(__name__)
_GLOB_CHARS = re.compile(r"[*?\[]")


def _to_arrow_table(result):
    if hasattr(result, "to_arrow_table"):
        return result.to_arrow_table()
    arrow = result.arrow()
    return arrow.read_all() if hasattr(arrow, "read_all") else arrow


def _is_presign_unsupported(exc: ApiException) -> bool:
    if getattr(exc, "status", None) != 400:
        return False
    body = getattr(exc, "body", "") or ""
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    return "presigned URL" in body and "not supported" in body


class ParquetQueryError(Exception):
    pass


class ParquetQuery:
    """Query parquet objects stored in Surogate Hub using DuckDB.

    Example::

        from surogate_hub_sdk import ApiClient, Configuration
        from surogate_hub_sdk.parquet import ParquetQuery

        client = ApiClient(Configuration(host="https://hub.example.com", ...))
        pq = ParquetQuery(client)

        tbl = pq.read("my-repo", "main", "events/*.parquet",
                      columns=["user_id", "ts"], limit=1000)
    """

    def __init__(
        self,
        api_client: Optional[ApiClient] = None,
        *,
        list_page_size: int = 1000,
        duckdb_threads: Optional[int] = None,
    ) -> None:
        try:
            import duckdb  # noqa: F401
        except ImportError as exc:  # pragma: no cover - import guard
            raise ParquetQueryError(
                "duckdb is required for parquet queries. Install with "
                "`pip install surogate-hub-sdk[parquet]`."
            ) from exc

        self._api_client = api_client or ApiClient.get_default()
        self._objects = ObjectsApi(self._api_client)
        self._list_page_size = list_page_size
        self._duckdb_threads = duckdb_threads
        # None = undecided; True/False = cached decision from the first attempt.
        self._presign_supported: Optional[bool] = None

    def read(
        self,
        repository: str,
        ref: str,
        path: str,
        *,
        columns: Optional[Sequence[str]] = None,
        filters: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "pyarrow.Table":
        """Read parquet data from ``path`` (exact, prefix, or glob).

        ``filters`` is a DuckDB WHERE clause (without the ``WHERE`` keyword),
        e.g. ``"country = 'US' AND ts > '2026-01-01'"``.
        """
        urls = self._resolve_urls(repository, ref, path)
        select = ", ".join(columns) if columns else "*"
        sql = f"SELECT {select} FROM read_parquet($urls)"
        if filters:
            sql += f" WHERE {filters}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        con = self._connect()
        try:
            return _to_arrow_table(con.execute(sql, {"urls": urls}))
        finally:
            con.close()

    def sql(
        self,
        repository: str,
        ref: str,
        sql: str,
        *,
        tables: Optional[Dict[str, str]] = None,
    ) -> "pyarrow.Table":
        """Run arbitrary DuckDB SQL.

        Each key in ``tables`` becomes a named view backed by ``read_parquet``
        over the URLs resolved from its path/glob. Reference them in ``sql``
        with ``{name}`` placeholders, which are substituted before execution.
        """
        con = self._connect()
        try:
            substitutions: Dict[str, str] = {}
            for name, spec in (tables or {}).items():
                if not name.isidentifier():
                    raise ParquetQueryError(f"invalid table alias: {name!r}")
                urls = self._resolve_urls(repository, ref, spec)
                view = f"_shub_{name}"
                con.register(view, con.from_parquet(urls))
                substitutions[name] = view
            rendered = sql.format(**substitutions) if substitutions else sql
            return _to_arrow_table(con.execute(rendered))
        finally:
            con.close()

    def _connect(self):
        import duckdb
        import fsspec

        con = duckdb.connect(database=":memory:")
        if self._duckdb_threads is not None:
            con.execute(f"PRAGMA threads={int(self._duckdb_threads)}")
        # DuckDB's httpfs caches the extension under $HOME; containers like
        # our Debian worker set HOME to /nonexistent which breaks INSTALL.
        con.execute("SET home_directory='/tmp'")
        # Always register shub:// so we can fall back without reconnecting.
        con.register_filesystem(fsspec.filesystem(PROTOCOL, api_client=self._api_client))
        # httpfs is only needed when we serve presigned HTTPS URLs to DuckDB;
        # the fsspec shub:// path doesn't use it. A nightly DuckDB build may
        # lack a matching httpfs binary (extensions.duckdb.org 404s per-hash);
        # log and continue so fsspec mode stays usable.
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except duckdb.Error as exc:
            log.warning(
                "DuckDB httpfs unavailable (%s); "
                "ParquetQuery will only work in fsspec/shub:// mode", exc,
            )
        return con

    def _resolve_urls(self, repository: str, ref: str, path: str) -> List[str]:
        listing = _GLOB_CHARS.search(path) is not None or path.endswith("/")
        if self._presign_supported is False:
            urls = self._shub_urls(repository, ref, path, listing)
        else:
            try:
                urls = self._presigned_urls(repository, ref, path, listing)
                self._presign_supported = True
            except ApiException as exc:
                if not _is_presign_unsupported(exc):
                    raise
                self._presign_supported = False
                urls = self._shub_urls(repository, ref, path, listing)
        if not urls:
            raise ParquetQueryError(
                f"no parquet objects matched path={path!r} in {repository}@{ref}"
            )
        return urls

    def _presigned_urls(
        self, repository: str, ref: str, path: str, listing: bool,
    ) -> List[str]:
        if listing:
            return list(self._list_presigned(repository, ref, path))
        stat = self._objects.stat_object(
            repository=repository, ref=ref, path=path, presign=True,
        )
        return [stat.physical_address]

    def _shub_urls(
        self, repository: str, ref: str, path: str, listing: bool,
    ) -> List[str]:
        if listing:
            return list(self._list_shub(repository, ref, path))
        return [f"{PROTOCOL_SCHEME}{repository}/{ref}/{path}"]

    def _list_presigned(
        self, repository: str, ref: str, pattern: str,
    ) -> Iterable[str]:
        for obj in self._iter_matching_objects(repository, ref, pattern, presign=True):
            yield obj.physical_address

    def _list_shub(
        self, repository: str, ref: str, pattern: str,
    ) -> Iterable[str]:
        for obj in self._iter_matching_objects(repository, ref, pattern, presign=False):
            yield f"{PROTOCOL_SCHEME}{repository}/{ref}/{obj.path}"

    def _iter_matching_objects(
        self, repository: str, ref: str, pattern: str, *, presign: bool,
    ):
        if pattern.endswith("/"):
            prefix, glob = pattern, None
        else:
            match = _GLOB_CHARS.search(pattern)
            prefix = pattern[: match.start()] if match else pattern
            glob = pattern

        after: Optional[str] = None
        while True:
            page = self._objects.list_objects(
                repository=repository,
                ref=ref,
                prefix=prefix,
                after=after,
                amount=self._list_page_size,
                presign=presign,
            )
            for obj in page.results or []:
                if obj.path_type != "object":
                    continue
                if glob is not None and not fnmatch.fnmatchcase(obj.path, glob):
                    continue
                yield obj
            if not page.pagination.has_more:
                return
            after = page.pagination.next_offset
