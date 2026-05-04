"""Tests for the worker's thin SDK wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock

from surogate_hub_sdk.exceptions import NotFoundException
from surogate_hub_sdk.models.object_stats import ObjectStats
from surogate_hub_sdk.models.object_stats_list import ObjectStatsList
from surogate_hub_sdk.models.pagination import Pagination

from surogate_hub_worker.config import Config
from surogate_hub_worker.hub import HubClient


def _config():
    return Config(
        hub_url="http://hub/api/v1",
        hub_access_key="AK",
        hub_secret_key="SK",
    )


def _page(results):
    return ObjectStatsList(
        pagination=Pagination(
            has_more=False, next_offset="", results=len(results), max_per_page=1000,
        ),
        results=results,
    )


def _object(path: str) -> ObjectStats:
    return ObjectStats(
        path=path,
        path_type="object",
        physical_address=f"mem://{path}",
        checksum="x",
        size_bytes=5,
        mtime=0,
    )


class RecordingRepos:
    def __init__(self):
        self.calls = []

    def get_repository_metadata(self, user, repository):
        self.calls.append(("metadata", user, repository))
        return {"type": "dataset"}


class RecordingBranches:
    def __init__(self):
        self.calls = []

    def get_branch(self, user, repository, branch):
        self.calls.append(("get", user, repository, branch))
        raise NotFoundException(status=404, reason="Not Found")

    def create_branch(self, user, repository, branch_creation):
        self.calls.append(("create", user, repository, branch_creation))
        return branch_creation.name


class RecordingCommits:
    def __init__(self):
        self.calls = []

    def get_commit(self, user, repository, commit_id):
        self.calls.append(("get", user, repository, commit_id))
        return MagicMock(id=commit_id)

    def commit(self, user, repository, branch, commit_creation):
        self.calls.append(("commit", user, repository, branch, commit_creation))
        return MagicMock(id="commit-id")


class RecordingObjects:
    def __init__(self):
        self.calls = []

    def list_objects(self, *, user, repository, ref, prefix, **kwargs):
        self.calls.append(("list", user, repository, ref, prefix, kwargs))
        return _page([_object("train/data.parquet"), _object("README.md")])

    def delete_objects(self, *, user, repository, branch, path_list):
        self.calls.append(("delete", user, repository, branch, path_list.paths))

    def upload_object(self, *, user, repository, branch, path, content, force):
        self.calls.append(("upload", user, repository, branch, path, content, force))
        return _object(path)

    def get_object(self, user, repository, ref, path):
        self.calls.append(("get", user, repository, ref, path))
        return bytearray(b"hello")


def test_hub_client_splits_namespaced_repository_for_generated_apis():
    hub = HubClient(_config(), api_client=MagicMock())
    hub.repos = RecordingRepos()
    hub.branches = RecordingBranches()
    hub.commits = RecordingCommits()
    hub.objects = RecordingObjects()

    assert hub.get_repo_metadata("owner/repo") == {"type": "dataset"}
    assert hub.get_commit("owner/repo", "abc").id == "abc"
    assert hub.ensure_branch("owner/repo", "_parquet_main", "abc") is True
    assert hub.shub_urls_for("owner/repo", "main") == [
        "sghub://owner%2Frepo/main/train/data.parquet"
    ]
    hub.delete_objects("owner/repo", "_parquet_main", ["old.parquet"])
    hub.upload("owner/repo", "_parquet_main", "train/data.parquet", b"hello")
    assert hub.read_object("owner/repo", "main", "dataset_dict.json") == b"hello"
    assert hub.commit("owner/repo", "_parquet_main", "msg", {"k": "v"}).id == "commit-id"

    assert hub.repos.calls == [("metadata", "owner", "repo")]
    assert hub.branches.calls[0] == ("get", "owner", "repo", "_parquet_main")
    assert hub.branches.calls[1][0:3] == ("create", "owner", "repo")
    assert hub.commits.calls[0] == ("get", "owner", "repo", "abc")
    assert hub.commits.calls[1][0:4] == ("commit", "owner", "repo", "_parquet_main")
    assert [call[0:3] for call in hub.objects.calls] == [
        ("list", "owner", "repo"),
        ("delete", "owner", "repo"),
        ("upload", "owner", "repo"),
        ("get", "owner", "repo"),
    ]
