"""fsspec filesystem backed by the Surogate Hub object API.

Registers a ``shub://`` protocol. Paths take the form
``shub://<repository>/<ref>/<object_path>`` — mirroring the
``lakefs-spec`` convention so users familiar with lakeFS don't get surprised.

Read-only, with HTTP-range-read support on open files, so
``datasets.load_dataset('shub://...')`` or DuckDB's
``read_parquet('shub://...')`` can stream without staging the whole
repository to local disk.

This is the transport that :class:`surogate_hub_sdk.parquet.ParquetQuery`
falls back to when the hub's blockstore doesn't support presigned URLs
(e.g. the ``local`` blockstore in dev setups).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import fsspec
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

from surogate_hub_sdk.api.objects_api import ObjectsApi
from surogate_hub_sdk.api.repositories_api import RepositoriesApi
from surogate_hub_sdk.api_client import ApiClient
from surogate_hub_sdk.configuration import Configuration
from surogate_hub_sdk.exceptions import NotFoundException


log = logging.getLogger(__name__)

PROTOCOL = "shub"
PROTOCOL_SCHEME = f"{PROTOCOL}://"


class SurogateHubFileSystem(AbstractFileSystem):
    """Read-only fsspec filesystem over the Surogate Hub object API.

    Paths: the portion after the protocol is ``<repo>/<ref>/<key>``. Root-,
    repo-, and ref-only paths are treated as directories; ``<key>`` can be
    a prefix (directory) or an exact object path (file).
    """

    protocol = PROTOCOL
    root_marker = "/"

    def __init__(
        self,
        host: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        *,
        api_client: Optional[ApiClient] = None,
        list_page_size: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if api_client is None:
            if not host:
                raise ValueError("must pass either host or api_client")
            api_client = ApiClient(
                Configuration(host=host, username=username, password=password)
            )
        self._api_client = api_client
        self._objects = ObjectsApi(api_client)
        self._repos = RepositoriesApi(api_client)
        self._list_page_size = list_page_size

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        stripped = super()._strip_protocol(path)
        if not stripped:
            return "/"
        return stripped

    def _parse(self, path: str) -> tuple[Optional[str], Optional[str], str]:
        stripped = self._strip_protocol(path).strip("/")
        if not stripped:
            return None, None, ""
        parts = stripped.split("/", 2)
        if len(parts) == 1:
            return parts[0], None, ""
        if len(parts) == 2:
            return parts[0], parts[1], ""
        return parts[0], parts[1], parts[2]

    def ls(
        self, path: str, detail: bool = True, **kwargs: Any,
    ) -> List[Dict[str, Any]] | List[str]:
        repo, ref, key = self._parse(path)
        if repo is None:
            return self._ls_repos(detail)
        if ref is None:
            return [] if detail else []
        prefix = f"{key}/" if key else ""
        items: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            page = self._objects.list_objects(
                repository=repo,
                ref=ref,
                prefix=prefix,
                delimiter="/",
                after=after,
                amount=self._list_page_size,
                presign=False,
            )
            for obj in page.results or []:
                full = f"/{repo}/{ref}/{obj.path}".rstrip("/")
                if obj.path_type == "common_prefix":
                    items.append({"name": full, "type": "directory", "size": 0})
                else:
                    items.append({
                        "name": full,
                        "type": "file",
                        "size": int(obj.size_bytes or 0),
                    })
            if not page.pagination.has_more:
                break
            after = page.pagination.next_offset
        return items if detail else [i["name"] for i in items]

    def _ls_repos(self, detail: bool) -> List[Dict[str, Any]] | List[str]:
        after: Optional[str] = None
        items: List[Dict[str, Any]] = []
        while True:
            page = self._repos.list_repositories(
                after=after, amount=self._list_page_size,
            )
            for r in page.results or []:
                items.append({
                    "name": f"/{r.id}", "type": "directory", "size": 0,
                })
            if not page.pagination.has_more:
                break
            after = page.pagination.next_offset
        return items if detail else [i["name"] for i in items]

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        repo, ref, key = self._parse(path)
        name = "/" + "/".join(p for p in (repo, ref, key) if p)
        if not key:
            return {"name": name, "type": "directory", "size": 0}
        try:
            stat = self._objects.stat_object(
                repository=repo, ref=ref, path=key, presign=False,
            )
        except NotFoundException:
            listing = self.ls(path, detail=True)
            if listing:
                return {"name": name, "type": "directory", "size": 0}
            raise FileNotFoundError(path)
        return {
            "name": f"/{repo}/{ref}/{stat.path}",
            "type": "file",
            "size": int(stat.size_bytes or 0),
            "checksum": stat.checksum,
            "mtime": stat.mtime,
        }

    def modified(self, path: str) -> datetime:
        mtime = self.info(path).get("mtime")
        if mtime is None:
            raise NotImplementedError(f"no mtime for {path}")
        return datetime.fromtimestamp(int(mtime), tz=timezone.utc)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> AbstractBufferedFile:
        if "w" in mode or "a" in mode or "x" in mode:
            raise NotImplementedError(f"{self.protocol!r} is read-only")
        repo, ref, key = self._parse(path)
        if not key:
            raise IsADirectoryError(path)
        info = self.info(path)
        return SurogateHubFile(
            self, path, repo=repo, ref=ref, key=key,
            size=info["size"], block_size=block_size,
            cache_options=cache_options, mode=mode,
        )


class SurogateHubFile(AbstractBufferedFile):
    def __init__(
        self,
        fs: SurogateHubFileSystem,
        path: str,
        *,
        repo: str,
        ref: str,
        key: str,
        size: int,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._repo = repo
        self._ref = ref
        self._key = key
        super().__init__(
            fs, path, mode=mode, block_size=block_size,
            cache_options=cache_options, size=size,
        )

    def _fetch_range(self, start: int, end: int) -> bytes:
        # HTTP Range headers are inclusive on both ends; AbstractBufferedFile
        # calls us with [start, end) in Python half-open style.
        if end <= start:
            return b""
        raw = self.fs._objects.get_object(
            repository=self._repo,
            ref=self._ref,
            path=self._key,
            range=f"bytes={start}-{end - 1}",
        )
        return bytes(raw)


def register() -> None:
    try:
        fsspec.register_implementation(PROTOCOL, SurogateHubFileSystem, clobber=False)
    except ValueError:
        # Already registered — safe no-op on re-import.
        pass
