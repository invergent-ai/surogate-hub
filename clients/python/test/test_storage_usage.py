"""Tests for the per-user storage and quota endpoints surfaced via AuthApi.

These cover the SDK-side shape of the new operations added by the storage-usage feature:

- ``GET    /auth/users/{userId}/storage``   → ``AuthApi.get_user_storage``
- ``PUT    /auth/users/{userId}/quota``     → ``AuthApi.set_user_quota``
- ``DELETE /auth/users/{userId}/quota``     → ``AuthApi.delete_user_quota``

We mock ``ApiClient`` so the tests exercise the generated method signatures, path-parameter
templating, body serialization, and Pydantic response deserialization end-to-end without making
an actual HTTP request.
"""

import datetime
import json
import unittest
from unittest.mock import MagicMock

from surogate_hub_sdk.api.auth_api import AuthApi
from surogate_hub_sdk.api_client import ApiClient
from surogate_hub_sdk.models.user_quota import UserQuota
from surogate_hub_sdk.models.user_storage import UserStorage
from surogate_hub_sdk.models.user_storage_repo import UserStorageRepo


def _api(call_api_return_value):
    """Build an AuthApi whose underlying call_api returns a canned response.

    The ApiClient's call_api is the lowest-level HTTP boundary; mocking it lets us assert on
    the constructed request without touching the network. response_deserialize is the
    pydantic-aware deserializer the generated code uses for the response.
    """
    client = ApiClient()
    # Intercept the HTTP boundary. The returned mock is later asked for .read() before the
    # response is deserialized — MagicMock answers .read() with another MagicMock by default,
    # which is fine because we also stub response_deserialize.
    client.call_api = MagicMock(return_value=MagicMock())
    # Intercept the deserializer so we control the parsed body without crafting a urllib3 response.
    client.response_deserialize = MagicMock(
        return_value=MagicMock(data=call_api_return_value)
    )
    return AuthApi(client), client


class GetUserStorageTest(unittest.TestCase):
    def test_path_and_response_shape(self):
        expected = UserStorage(
            user="alice",
            bytes_used=1234,
            quota_bytes=10_000,
            bytes_remaining=8766,
            repositories=[
                UserStorageRepo(name="training", bytes_used=1000),
                UserStorageRepo(name="evals", bytes_used=234),
            ],
            last_reconciled_at=datetime.datetime(2026, 5, 16, 10, 23, 0, tzinfo=datetime.timezone.utc),
            is_estimate=False,
        )
        api, client = _api(expected)

        got = api.get_user_storage("alice")

        # Returns the deserialized UserStorage instance.
        self.assertIsInstance(got, UserStorage)
        self.assertEqual(got.user, "alice")
        self.assertEqual(got.bytes_used, 1234)
        self.assertEqual(got.quota_bytes, 10_000)
        self.assertEqual(got.bytes_remaining, 8766)
        self.assertEqual(len(got.repositories), 2)
        self.assertFalse(got.is_estimate)

        # The path serializer ran exactly once with the URL template + userId path parameter.
        call_api_args, _ = client.call_api.call_args
        # call_api is called with positional args from param_serialize: method, url, headers, body, post_params
        # The serialized URL must contain the userId we passed.
        self.assertIn("alice", call_api_args[1], f"expected userId in URL; got {call_api_args[1]!r}")
        self.assertEqual(call_api_args[0], "GET")

    def test_unlimited_user_has_no_quota_fields(self):
        # Server returns no quota_bytes / bytes_remaining when the user is unlimited.
        expected = UserStorage(
            user="bob",
            bytes_used=0,
            quota_bytes=None,
            bytes_remaining=None,
            repositories=[],
            last_reconciled_at=None,
            is_estimate=True,
        )
        api, _ = _api(expected)
        got = api.get_user_storage("bob")

        self.assertIsNone(got.quota_bytes)
        self.assertIsNone(got.bytes_remaining)
        self.assertIsNone(got.last_reconciled_at)
        self.assertTrue(got.is_estimate)
        self.assertEqual(got.repositories, [])


class SetUserQuotaTest(unittest.TestCase):
    def test_serializes_quota_bytes_in_body_with_put(self):
        api, client = _api(None)  # 204 No Content — response data is None

        api.set_user_quota("alice", UserQuota(quota_bytes=12345))

        # call_api positional args are (method, url, headers, body, post_params, ...).
        call_api_args, _ = client.call_api.call_args
        method, url, headers, body = call_api_args[0], call_api_args[1], call_api_args[2], call_api_args[3]
        self.assertEqual(method, "PUT")
        self.assertIn("alice", url)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        # Body is the model's dict representation: param_serialize hands the dict to the HTTP
        # layer; urllib3 serializes it via json.dumps before sending.
        self.assertEqual(body, {"quota_bytes": 12345})
        # Sanity: round-trip the dict back through json to confirm it is JSON-encodable.
        self.assertEqual(json.loads(json.dumps(body)), {"quota_bytes": 12345})

    def test_quota_bytes_zero_is_accepted_by_the_model(self):
        # The spec/docs say quota_bytes=0 is an intentional "lock the user out" value;
        # the model must validate it (minimum=0 in swagger).
        body = UserQuota(quota_bytes=0)
        self.assertEqual(body.quota_bytes, 0)

    def test_quota_bytes_negative_rejected_by_pydantic(self):
        # The swagger schema declares minimum: 0; pydantic enforces it on construction so the
        # SDK fails fast before the round-trip rather than letting the server return 400.
        with self.assertRaises(Exception):
            UserQuota(quota_bytes=-1)


class DeleteUserQuotaTest(unittest.TestCase):
    def test_path_and_method(self):
        api, client = _api(None)
        api.delete_user_quota("alice")
        call_api_args, _ = client.call_api.call_args
        self.assertEqual(call_api_args[0], "DELETE")
        self.assertIn("alice", call_api_args[1])


class UserStorageModelTest(unittest.TestCase):
    """Round-trip tests for UserStorage so the spec's response shape stays in sync with the SDK."""

    def test_round_trip_with_quota(self):
        payload = {
            "user": "alice",
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
        model = UserStorage.from_dict(payload)
        self.assertEqual(model.user, "alice")
        self.assertEqual(model.bytes_used, 1234567890)
        self.assertEqual(model.quota_bytes, 10737418240)
        self.assertEqual(len(model.repositories), 2)
        self.assertEqual(model.repositories[0].name, "training-data")
        # Re-serialize round-trip preserves keys.
        again = model.to_dict()
        self.assertEqual(again["bytes_used"], 1234567890)
        self.assertEqual(again["quota_bytes"], 10737418240)

    def test_round_trip_unlimited(self):
        # quota fields and last_reconciled_at can all be absent (unlimited / pre-reconcile).
        payload = {
            "user": "bob",
            "bytes_used": 0,
            "repositories": [],
            "is_estimate": True,
        }
        model = UserStorage.from_dict(payload)
        self.assertIsNone(model.quota_bytes)
        self.assertIsNone(model.bytes_remaining)
        self.assertIsNone(model.last_reconciled_at)
        self.assertTrue(model.is_estimate)


if __name__ == "__main__":
    unittest.main()
