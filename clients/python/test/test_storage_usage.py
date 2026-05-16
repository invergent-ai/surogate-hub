"""Tests for the per-owner storage and quota endpoints surfaced via StorageApi.

These cover the SDK-side shape of the operations added by the storage-usage feature:

- ``GET    /storage/owners/{owner}``        → ``StorageApi.get_owner_storage``
- ``PUT    /storage/owners/{owner}/quota``  → ``StorageApi.set_owner_quota``
- ``DELETE /storage/owners/{owner}/quota``  → ``StorageApi.delete_owner_quota``

The path's ``{owner}`` is the repository owner namespace — the first path segment of every
repo id, e.g. a synthetic project workspace id like ``p-39264d5a``. Not necessarily a
registered hub auth user.

We mock ``ApiClient`` so the tests exercise the generated method signatures, path-parameter
templating, body serialization, and Pydantic response deserialization end-to-end without making
an actual HTTP request.
"""

import datetime
import json
import unittest
from unittest.mock import MagicMock

from surogate_hub_sdk.api.storage_api import StorageApi
from surogate_hub_sdk.api_client import ApiClient
from surogate_hub_sdk.models.owner_quota import OwnerQuota
from surogate_hub_sdk.models.owner_storage import OwnerStorage
from surogate_hub_sdk.models.owner_storage_repo import OwnerStorageRepo


def _api(call_api_return_value):
    """Build a StorageApi whose underlying call_api returns a canned response.

    The ApiClient's call_api is the lowest-level HTTP boundary; mocking it lets us assert on
    the constructed request without touching the network. response_deserialize is the
    pydantic-aware deserializer the generated code uses for the response.
    """
    client = ApiClient()
    # Intercept the HTTP boundary. The returned mock is later asked for .read() before the
    # response is deserialized — MagicMock answers .read() with another MagicMock by default,
    # which is fine because we also stub response_deserialize.
    client.call_api = MagicMock(return_value=MagicMock())
    client.response_deserialize = MagicMock(
        return_value=MagicMock(data=call_api_return_value)
    )
    return StorageApi(client), client


class GetOwnerStorageTest(unittest.TestCase):
    def test_path_and_response_shape(self):
        expected = OwnerStorage(
            owner="p-39264d5a",
            bytes_used=1234,
            quota_bytes=10_000,
            bytes_remaining=8766,
            repositories=[
                OwnerStorageRepo(name="training", bytes_used=1000),
                OwnerStorageRepo(name="evals", bytes_used=234),
            ],
            last_reconciled_at=datetime.datetime(2026, 5, 16, 10, 23, 0, tzinfo=datetime.timezone.utc),
            is_estimate=False,
        )
        api, client = _api(expected)

        got = api.get_owner_storage("p-39264d5a")

        self.assertIsInstance(got, OwnerStorage)
        self.assertEqual(got.owner, "p-39264d5a")
        self.assertEqual(got.bytes_used, 1234)
        self.assertEqual(got.quota_bytes, 10_000)
        self.assertEqual(got.bytes_remaining, 8766)
        self.assertEqual(len(got.repositories), 2)
        self.assertFalse(got.is_estimate)

        call_api_args, _ = client.call_api.call_args
        # The URL must contain the owner path segment.
        self.assertIn("p-39264d5a", call_api_args[1], f"expected owner in URL; got {call_api_args[1]!r}")
        self.assertEqual(call_api_args[0], "GET")
        # And it must be the new /storage/owners/ path, not the old /auth/users/ one.
        self.assertIn("/storage/owners/", call_api_args[1])
        self.assertNotIn("/auth/users/", call_api_args[1])

    def test_unlimited_owner_has_no_quota_fields(self):
        expected = OwnerStorage(
            owner="bob",
            bytes_used=0,
            quota_bytes=None,
            bytes_remaining=None,
            repositories=[],
            last_reconciled_at=None,
            is_estimate=True,
        )
        api, _ = _api(expected)
        got = api.get_owner_storage("bob")

        self.assertIsNone(got.quota_bytes)
        self.assertIsNone(got.bytes_remaining)
        self.assertIsNone(got.last_reconciled_at)
        self.assertTrue(got.is_estimate)
        self.assertEqual(got.repositories, [])


class SetOwnerQuotaTest(unittest.TestCase):
    def test_serializes_quota_bytes_in_body_with_put(self):
        api, client = _api(None)  # 204 No Content — response data is None

        api.set_owner_quota("p-39264d5a", OwnerQuota(quota_bytes=12345))

        call_api_args, _ = client.call_api.call_args
        method, url, headers, body = call_api_args[0], call_api_args[1], call_api_args[2], call_api_args[3]
        self.assertEqual(method, "PUT")
        self.assertIn("p-39264d5a", url)
        self.assertIn("/storage/owners/", url)
        self.assertIn("/quota", url)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        self.assertEqual(body, {"quota_bytes": 12345})
        self.assertEqual(json.loads(json.dumps(body)), {"quota_bytes": 12345})

    def test_quota_bytes_zero_is_accepted_by_the_model(self):
        # The spec/docs say quota_bytes=0 is an intentional "lock the owner out" value;
        # the model must validate it (minimum=0 in swagger).
        body = OwnerQuota(quota_bytes=0)
        self.assertEqual(body.quota_bytes, 0)

    def test_quota_bytes_negative_rejected_by_pydantic(self):
        # The swagger schema declares minimum: 0; pydantic enforces it on construction.
        with self.assertRaises(Exception):
            OwnerQuota(quota_bytes=-1)


class DeleteOwnerQuotaTest(unittest.TestCase):
    def test_path_and_method(self):
        api, client = _api(None)
        api.delete_owner_quota("p-39264d5a")
        call_api_args, _ = client.call_api.call_args
        self.assertEqual(call_api_args[0], "DELETE")
        self.assertIn("p-39264d5a", call_api_args[1])
        self.assertIn("/storage/owners/", call_api_args[1])


class OwnerStorageModelTest(unittest.TestCase):
    """Round-trip tests for OwnerStorage so the spec's response shape stays in sync with the SDK."""

    def test_round_trip_with_quota(self):
        payload = {
            "owner": "p-39264d5a",
            "bytes_used": 1234567890,
            "quota_bytes": 10737418240,
            "bytes_remaining": 9502850350,
            "repositories": [
                {"name": "training-data", "bytes_used": 900000000},
                {"name": "evals", "bytes_used": 334567890},
            ],
            "last_reconciled_at": "2026-05-16T10:23:00Z",
            "is_estimate": False,
        }
        model = OwnerStorage.from_dict(payload)
        self.assertEqual(model.owner, "p-39264d5a")
        self.assertEqual(model.bytes_used, 1234567890)
        self.assertEqual(model.quota_bytes, 10737418240)
        self.assertEqual(len(model.repositories), 2)
        self.assertEqual(model.repositories[0].name, "training-data")
        again = model.to_dict()
        self.assertEqual(again["bytes_used"], 1234567890)
        self.assertEqual(again["quota_bytes"], 10737418240)
        self.assertEqual(again["owner"], "p-39264d5a")

    def test_round_trip_unlimited(self):
        payload = {
            "owner": "bob",
            "bytes_used": 0,
            "repositories": [],
            "is_estimate": True,
        }
        model = OwnerStorage.from_dict(payload)
        self.assertIsNone(model.quota_bytes)
        self.assertIsNone(model.bytes_remaining)
        self.assertIsNone(model.last_reconciled_at)
        self.assertTrue(model.is_estimate)


if __name__ == "__main__":
    unittest.main()
