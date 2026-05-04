"""Shared test fakes.

A tiny in-memory fake of the subset of ``ObjectsApi`` our code actually
uses — ``list_objects`` (with ``delimiter`` / pagination), ``stat_object``,
and ``get_object`` (with byte-range reads). Sidesteps a live hub without
faking the whole SDK.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from surogate_hub_sdk.exceptions import NotFoundException
from surogate_hub_sdk.models.object_stats import ObjectStats
from surogate_hub_sdk.models.object_stats_list import ObjectStatsList
from surogate_hub_sdk.models.pagination import Pagination


@dataclass
class _Obj:
    content: bytes


def _object_stats(path: str, content: bytes) -> ObjectStats:
    return ObjectStats(
        path=path,
        path_type="object",
        physical_address=f"mem://{path}",
        checksum="x",
        size_bytes=len(content),
        mtime=0,
    )


class InMemoryObjectStore:
    def __init__(self, objects: Dict[str, bytes]) -> None:
        self._objects = {k: _Obj(v) for k, v in objects.items()}

    def list_objects(
        self, *, user=None, repository, ref, prefix, delimiter=None, after=None,
        amount=None, presign=None,
    ) -> ObjectStatsList:
        results: List[ObjectStats] = []
        seen_prefixes: set = set()
        matching = sorted(
            p for p in self._objects if p.startswith(prefix or "")
        )
        for p in matching:
            tail = p[len(prefix):]
            if delimiter and delimiter in tail:
                common = prefix + tail.split(delimiter, 1)[0] + delimiter
                if common in seen_prefixes:
                    continue
                seen_prefixes.add(common)
                results.append(ObjectStats(
                    path=common, path_type="common_prefix",
                    physical_address="", checksum="", size_bytes=0, mtime=0,
                ))
            else:
                results.append(_object_stats(p, self._objects[p].content))
        return ObjectStatsList(
            pagination=Pagination(
                has_more=False, next_offset="",
                results=len(results), max_per_page=amount or 1000,
            ),
            results=results,
        )

    def stat_object(self, *, user=None, repository, ref, path, presign=None):
        if path not in self._objects:
            raise NotFoundException(status=404, reason="Not Found")
        return _object_stats(path, self._objects[path].content)

    def get_object(self, *, user=None, repository, ref, path, range=None, **_):
        if path not in self._objects:
            raise NotFoundException(status=404, reason="Not Found")
        data = self._objects[path].content
        if range:
            assert range.startswith("bytes=")
            start_s, end_s = range[len("bytes="):].split("-")
            return bytearray(data[int(start_s): int(end_s) + 1])
        return bytearray(data)
