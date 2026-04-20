"""Processor decides whether to skip; when it proceeds, it must convert,
commit parquet shards, and compute stats."""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from surogate_hub_worker.config import Config
from surogate_hub_worker.converter import ConversionResult, ConvertedSplit
from surogate_hub_worker.processor import Event, EventProcessor, Skip


def _config():
    return Config(
        hub_url="http://hub",
        hub_access_key="AK",
        hub_secret_key="SK",
    )


def _parquet_bytes(table: pa.Table) -> bytes:
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _mock_hub(
    repo_md=None, existing_manifest=None, parquet_commit_id="PQ_COMMIT",
    stats_commit_id="STATS_COMMIT",
):
    """A MagicMock HubClient whose ``shub_urls_for`` returns whatever
    the caller attaches to ``hub._urls_by_ref[ref]`` — tests use this to
    pin different URL sets per ref (parquet branch vs. data branch)."""
    hub = MagicMock()
    hub.get_repo_metadata.return_value = repo_md or {}
    hub._urls_by_ref = {}
    hub.shub_urls_for.side_effect = lambda repo, ref: hub._urls_by_ref.get(ref, [])
    hub.read_object.return_value = (
        existing_manifest.encode() if isinstance(existing_manifest, str) else existing_manifest
    )
    hub.ensure_branch.return_value = None
    hub.list_all_objects.return_value = []
    hub.delete_objects.return_value = None

    commits = iter([
        MagicMock(id=parquet_commit_id),
        MagicMock(id=stats_commit_id),
    ])
    hub.commit.side_effect = lambda **kwargs: next(commits)
    return hub


def _patch_converter(processor, *splits: ConvertedSplit, error=None):
    """Override the processor's converter with a fake that returns fixed splits."""
    fake = MagicMock()
    fake.convert.return_value = ConversionResult(
        splits=list(splits), configs_seen=["default"], error=error,
    )
    processor._converter = fake
    return fake


def test_event_from_payload_strict():
    event = Event.from_payload({
        "event_type": "post-commit",
        "repository_id": "r",
        "branch_id": "main",
        "commit_id": "abc",
        "commit_metadata": {"k": "v"},
    })
    assert event.commit_metadata == {"k": "v"}


def test_event_from_payload_missing_fields():
    with pytest.raises(ValueError):
        Event.from_payload({"event_type": "post-commit"})


def test_skip_when_not_post_commit():
    p = EventProcessor(_config(), _mock_hub())
    with pytest.raises(Skip):
        p.process(Event("r", "main", "abc", "pre-commit", {}))


def test_skip_self_stats_commit():
    p = EventProcessor(_config(), _mock_hub(repo_md={"type": "dataset"}))
    with pytest.raises(Skip):
        p.process(Event("r", "main", "abc", "post-commit", {"stats_commit": "true"}))


def test_skip_self_parquet_commit():
    p = EventProcessor(_config(), _mock_hub(repo_md={"type": "dataset"}))
    with pytest.raises(Skip):
        p.process(Event("r", "main", "abc", "post-commit", {"parquet_conversion": "true"}))


def test_skip_when_commit_on_stats_branch():
    p = EventProcessor(_config(), _mock_hub(repo_md={"type": "dataset"}))
    with pytest.raises(Skip):
        p.process(Event("r", "_stats_main", "abc", "post-commit", {}))


def test_skip_when_commit_on_parquet_branch():
    p = EventProcessor(_config(), _mock_hub(repo_md={"type": "dataset"}))
    with pytest.raises(Skip):
        p.process(Event("r", "_parquet_main", "abc", "post-commit", {}))


def test_skip_when_repo_not_dataset():
    p = EventProcessor(_config(), _mock_hub(repo_md={"type": "model"}))
    with pytest.raises(Skip):
        p.process(Event("r", "main", "abc", "post-commit", {}))


def test_skip_when_conversion_yields_nothing():
    hub = _mock_hub(repo_md={"type": "dataset"})
    p = EventProcessor(_config(), hub)
    _patch_converter(p, error="unsupported layout")
    result = p.process(Event("r", "main", "abc", "post-commit", {}))
    assert result is None
    hub.commit.assert_not_called()


def test_end_to_end_without_text_column(tmp_path):
    hub = _mock_hub(repo_md={"type": "dataset"})
    p = EventProcessor(_config(), hub)
    split_bytes = _parquet_bytes(pa.table({"x": [1, 1, 2, 3], "y": ["a", "a", "b", "c"]}))
    _patch_converter(p, ConvertedSplit(
        config_name="default", split_name="train",
        content=split_bytes, row_count=4,
    ))
    pq_path = tmp_path / "converted.parquet"
    pq_path.write_bytes(split_bytes)
    hub._urls_by_ref["PQ_COMMIT"] = [f"file://{pq_path}"]

    result = p.process(Event("r", "main", "COMMIT_XYZ", "post-commit", {}))
    assert result is not None
    assert result.stats_ref == "_stats_main"
    paths = sorted(result.written)
    assert "manifest.json" in paths
    assert any(p.startswith("summary/") for p in paths)
    assert any(p.startswith("duplicates/") for p in paths)
    assert not any(p.startswith("pii/") for p in paths)

    # Two commits: parquet branch, then stats branch.
    assert hub.commit.call_count == 2
    parquet_call, stats_call = hub.commit.call_args_list
    assert parquet_call.kwargs["branch"] == "_parquet_main"
    assert parquet_call.kwargs["metadata"]["parquet_conversion"] == "true"
    assert parquet_call.kwargs["metadata"]["source_commit"] == "COMMIT_XYZ"
    assert stats_call.kwargs["branch"] == "_stats_main"
    assert stats_call.kwargs["metadata"]["stats_commit"] == "true"

    parquet_uploads = {
        call.args[2] for call in hub.upload.call_args_list
        if call.args[1] == "_parquet_main"
    }
    assert parquet_uploads == {
        "train/data.parquet",
        "train/dataset_info.json",
        "dataset_dict.json",
    }


def test_end_to_end_with_text_column(tmp_path):
    hub = _mock_hub(repo_md={"type": "dataset", "dataset.text_column": "text"})
    p = EventProcessor(_config(), hub)
    split_bytes = _parquet_bytes(pa.table({
        "text": [
            "hello alice@example.com",
            "this is clean",
            "ssn 111-22-3333",
        ],
    }))
    _patch_converter(p, ConvertedSplit(
        config_name="default", split_name="train",
        content=split_bytes, row_count=3,
    ))
    pq_path = tmp_path / "converted.parquet"
    pq_path.write_bytes(split_bytes)
    hub._urls_by_ref["PQ_COMMIT"] = [f"file://{pq_path}"]

    result = p.process(Event("r", "main", "COMMIT_ABC", "post-commit", {}))
    paths = sorted(result.written)
    assert any(p.startswith("pii/") for p in paths)
    assert any(p.startswith("token_lengths/") for p in paths)


def test_parquet_branch_is_cleared_of_inherited_content(tmp_path):
    """On commit, the parquet branch should contain only our outputs —
    no leftover .arrow / state.json inherited from the data branch, and
    no previous-layout paths like default/train/data.parquet."""
    from surogate_hub_sdk.models.object_stats import ObjectStats

    hub = _mock_hub(repo_md={"type": "dataset"})
    stale = [
        ObjectStats(path=p, path_type="object", physical_address="",
                    checksum="", size_bytes=0, mtime=0)
        for p in [
            "train/data-00000-of-00001.arrow",
            "train/state.json",
            "test/state.json",
            "default/train/data.parquet",  # previous-layout leftover
            "surogate_info.json",
        ]
    ]
    hub.list_all_objects.return_value = iter(stale)

    p = EventProcessor(_config(), hub)
    split_bytes = _parquet_bytes(pa.table({"x": [1, 2]}))
    _patch_converter(p, ConvertedSplit(
        config_name="default", split_name="train",
        content=split_bytes, row_count=2,
    ))
    pq_path = tmp_path / "converted.parquet"
    pq_path.write_bytes(split_bytes)
    hub._urls_by_ref["PQ_COMMIT"] = [f"file://{pq_path}"]

    p.process(Event("r", "main", "COMMIT_XYZ", "post-commit", {}))

    deletes = [call.args for call in hub.delete_objects.call_args_list]
    assert len(deletes) == 1
    repo, branch, paths = deletes[0]
    assert repo == "r"
    assert branch == "_parquet_main"
    # Everything stale is queued for deletion; our about-to-write paths are spared.
    assert set(paths) == {
        "train/data-00000-of-00001.arrow",
        "train/state.json",
        "test/state.json",
        "default/train/data.parquet",
        "surogate_info.json",
    }


def test_merges_with_existing_manifest(tmp_path):
    existing = {
        "manifest_version": 1,
        "stats": {
            "legacy_stat": {
                "version": "v1",
                "source_commit": "OLD",
                "produced_at": "2020-01-01T00:00:00+00:00",
                "producer": "old-tool@0.0.1",
            },
        },
    }
    hub = _mock_hub(repo_md={"type": "dataset"}, existing_manifest=json.dumps(existing))
    p = EventProcessor(_config(), hub)
    split_bytes = _parquet_bytes(pa.table({"x": [1, 1]}))
    _patch_converter(p, ConvertedSplit(
        config_name="default", split_name="train",
        content=split_bytes, row_count=2,
    ))
    pq_path = tmp_path / "converted.parquet"
    pq_path.write_bytes(split_bytes)
    hub._urls_by_ref["PQ_COMMIT"] = [f"file://{pq_path}"]

    p.process(Event("r", "main", "NEW", "post-commit", {}))

    uploads = {call.args[2]: call.args[3] for call in hub.upload.call_args_list}
    merged = json.loads(uploads["manifest.json"].decode())
    assert "legacy_stat" in merged["stats"]
    assert "summary" in merged["stats"]
    assert "duplicates" in merged["stats"]
