"""Thin wrapper around the Surogate Hub SDK for the worker's needs.

Owns the :class:`ApiClient` and exposes only the operations the worker
performs: resolving presigned URLs, reading repo metadata, creating branches,
uploading objects, and committing.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, Iterable, List, Optional
from urllib.parse import quote

from surogate_hub_worker.fs import PROTOCOL_SCHEME

from surogate_hub_sdk import (
    ApiClient,
    BranchCreation,
    CommitCreation,
    Configuration,
    PathList,
)
from surogate_hub_sdk.api.branches_api import BranchesApi
from surogate_hub_sdk.api.commits_api import CommitsApi
from surogate_hub_sdk.api.objects_api import ObjectsApi
from surogate_hub_sdk.api.repositories_api import RepositoriesApi
from surogate_hub_sdk.exceptions import NotFoundException
from surogate_hub_sdk.models.commit import Commit
from surogate_hub_sdk.models.object_stats import ObjectStats
from surogate_hub_sdk.repository import split_repository_id

from surogate_hub_worker.config import Config


log = logging.getLogger(__name__)


class HubClient:
    def __init__(self, config: Config, api_client: Optional[ApiClient] = None) -> None:
        self._config = config
        if api_client is None:
            cfg = Configuration(
                host=config.hub_url,
                username=config.hub_access_key,
                password=config.hub_secret_key,
            )
            api_client = ApiClient(cfg)
        self._api_client = api_client
        self.repos = RepositoriesApi(api_client)
        self.branches = BranchesApi(api_client)
        self.commits = CommitsApi(api_client)
        self.objects = ObjectsApi(api_client)

    @property
    def api_client(self) -> ApiClient:
        return self._api_client

    def get_repo_metadata(self, repository: str) -> Dict[str, str]:
        try:
            user, repo_name = split_repository_id(repository)
            return dict(self.repos.get_repository_metadata(user, repo_name) or {})
        except NotFoundException:
            return {}

    def get_commit(self, repository: str, commit_id: str) -> Commit:
        user, repo_name = split_repository_id(repository)
        return self.commits.get_commit(user, repo_name, commit_id)

    def list_all_objects(
        self, repository: str, ref: str, prefix: str = "",
    ) -> Iterable[ObjectStats]:
        """Iterate every object (not common-prefixes) under a ref."""
        yield from self._iter_objects(repository, ref, prefix, lambda _: True)

    def list_parquet_objects(
        self, repository: str, ref: str, prefix: str = "",
    ) -> Iterable[ObjectStats]:
        """Iterate every ``.parquet`` object under a ref."""
        yield from self._iter_objects(
            repository, ref, prefix, lambda o: o.path.endswith(".parquet"),
        )

    def delete_objects(self, repository: str, branch: str, paths: List[str]) -> None:
        """Batch-delete a set of paths. Missing paths are silently tolerated."""
        if not paths:
            return
        user, repo_name = split_repository_id(repository)
        self.objects.delete_objects(
            user=user,
            repository=repo_name,
            branch=branch,
            path_list=PathList(paths=paths),
        )

    def shub_urls_for(self, repository: str, ref: str) -> List[str]:
        """Return ``shub://<repo>/<ref>/<path>`` URLs for every parquet object.

        Uses the worker's own fsspec filesystem for reads — sidesteps the
        local-blockstore limitation that presigned URLs can't be generated
        against non-S3 backends.
        """
        encoded_repo = quote(repository, safe="")
        return [
            f"{PROTOCOL_SCHEME}{encoded_repo}/{ref}/{o.path}"
            for o in self.list_parquet_objects(repository, ref)
        ]

    def _iter_objects(
        self,
        repository: str,
        ref: str,
        prefix: str,
        match: Callable[[ObjectStats], bool],
    ) -> Iterable[ObjectStats]:
        after: Optional[str] = None
        user, repo_name = split_repository_id(repository)
        while True:
            page = self.objects.list_objects(
                user=user,
                repository=repo_name,
                ref=ref,
                prefix=prefix,
                after=after,
                amount=self._config.list_page_size,
                presign=False,
            )
            for obj in page.results or []:
                if obj.path_type == "object" and match(obj):
                    yield obj
            if not page.pagination.has_more:
                return
            after = page.pagination.next_offset

    def ensure_branch(self, repository: str, branch: str, source: str) -> bool:
        """Create ``branch`` from ``source`` if missing. Returns True if created."""
        user, repo_name = split_repository_id(repository)
        try:
            self.branches.get_branch(user, repo_name, branch)
            return False
        except NotFoundException:
            pass
        self.branches.create_branch(
            user,
            repo_name,
            BranchCreation(name=branch, source=source, hidden=True),
        )
        log.info("created branch %s from %s", branch, source)
        return True

    def upload(self, repository: str, branch: str, path: str, data: bytes) -> None:
        user, repo_name = split_repository_id(repository)
        self.objects.upload_object(
            user=user,
            repository=repo_name,
            branch=branch,
            path=path,
            content=("content", data),
            force=True,
        )

    def commit(
        self,
        repository: str,
        branch: str,
        message: str,
        metadata: Dict[str, str],
    ) -> Commit:
        user, repo_name = split_repository_id(repository)
        return self.commits.commit(
            user=user,
            repository=repo_name,
            branch=branch,
            commit_creation=CommitCreation(
                message=message, metadata=metadata, allow_empty=True,
            ),
        )

    def read_object(self, repository: str, ref: str, path: str) -> Optional[bytes]:
        try:
            user, repo_name = split_repository_id(repository)
            return bytes(self.objects.get_object(user, repo_name, ref, path))
        except NotFoundException:
            return None
