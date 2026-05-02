import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from surogate_hub_sdk.configuration import Configuration
from surogate_hub_sdk.models.object_stats import ObjectStats

from test_xet_client import FakeHfXet, FakeStagingApi


class FakeInternalApi:
    def __init__(self, upload_mode="regular"):
        self.upload_mode = upload_mode
        self.calls = []

    def upload_object_preflight(self, repository, branch, path, size_bytes=None, **kwargs):
        self.calls.append(
            {
                "repository": repository,
                "branch": branch,
                "path": path,
                "size_bytes": size_bytes,
                "kwargs": kwargs,
            }
        )
        return SimpleNamespace(upload_mode=self.upload_mode)


class TestXetObjectsApi(unittest.TestCase):
    def api(self, hf_xet=None, upload_mode="regular"):
        from surogate_hub_sdk.xet_objects_api import XetObjectsApi

        configuration = Configuration(
            host="http://sghub.example/api/v1",
            username="access",
            password="secret",
        )
        api_client = SimpleNamespace(configuration=configuration)
        api = XetObjectsApi(
            api_client,
            hf_xet_module=hf_xet or FakeHfXet(),
            token_info=("token", 4102444800),
            token_refresher=lambda: ("token", 4102444800),
        )
        api._staging_api = FakeStagingApi()
        api._internal_api = FakeInternalApi(upload_mode=upload_mode)
        return api

    def test_hub_client_uses_xet_objects_api_by_default(self):
        from surogate_hub_sdk.client import HubClient
        from surogate_hub_sdk.xet_objects_api import XetObjectsApi

        configuration = Configuration(
            host="http://sghub.example/api/v1",
            username="access",
            password="secret",
        )

        client = HubClient(configuration=configuration)

        self.assertIsInstance(client.objects_api, XetObjectsApi)

    def test_upload_object_uses_regular_upload_when_preflight_returns_regular(self):
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="regular")

        with patch.object(ObjectsApi, "upload_object", return_value="regular-result") as regular_upload:
            result = api.upload_object("repo", "main", "small.txt", content=b"small")

        self.assertEqual(result, "regular-result")
        regular_upload.assert_called_once()
        self.assertEqual(hf_xet.upload_calls, [])
        self.assertEqual(api._internal_api.calls[0]["size_bytes"], 5)

    def test_upload_object_uses_xet_for_path_content_when_preflight_returns_xet(self):
        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="xet")
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"model bytes")
            local_path = f.name
        self.addCleanup(lambda: os.path.exists(local_path) and os.unlink(local_path))

        result = api.upload_object("repo", "main", "models/model.bin", content=local_path)

        self.assertEqual(hf_xet.upload_calls[0]["file_paths"], [local_path])
        self.assertEqual(hf_xet.upload_calls[0]["endpoint"], "http://sghub.example/xet")
        link = api._staging_api.calls[0]
        self.assertEqual(link["repository"], "repo")
        self.assertEqual(link["branch"], "main")
        self.assertEqual(link["path"], "models/model.bin")
        self.assertEqual(link["staging_metadata"].staging.physical_address, "xet://file-hash")
        self.assertEqual(result.path, "models/model.bin")

    def test_upload_object_spools_bytes_to_xet(self):
        class InspectingHfXet(FakeHfXet):
            def upload_files(self, file_paths, *args):
                with open(file_paths[0], "rb") as f:
                    self.uploaded_bytes = f.read()
                return super().upload_files(file_paths, *args)

        hf_xet = InspectingHfXet()
        api = self.api(hf_xet, upload_mode="xet")

        api.upload_object("repo", "main", "models/model.bin", content=b"model bytes")

        self.assertEqual(hf_xet.uploaded_bytes, b"model bytes")
        self.assertEqual(api._internal_api.calls[0]["size_bytes"], len(b"model bytes"))

    def test_upload_object_skips_preflight_for_unsupported_xet_options(self):
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet, upload_mode="xet")

        with patch.object(ObjectsApi, "upload_object", return_value="regular-result") as regular_upload:
            result = api.upload_object("repo", "main", "archive.bin", storage_class="STANDARD", content=b"data")

        self.assertEqual(result, "regular-result")
        regular_upload.assert_called_once()
        self.assertEqual(api._internal_api.calls, [])
        self.assertEqual(hf_xet.upload_calls, [])

    def test_get_object_uses_xet_for_xet_physical_address(self):
        class DownloadingHfXet(FakeHfXet):
            def download_files(self, files, *args):
                with open(files[0].destination_path, "wb") as f:
                    f.write(b"downloaded model")
                return super().download_files(files, *args)

        hf_xet = DownloadingHfXet()
        api = self.api(hf_xet)
        api.stat_object = lambda repository, ref, path: ObjectStats(
            path=path,
            path_type="object",
            physical_address="xet://file-hash",
            checksum="file-hash",
            size_bytes=16,
            mtime=0,
        )

        data = api.get_object("repo", "main", "models/model.bin")

        self.assertEqual(data, b"downloaded model")
        call = hf_xet.download_calls[0]
        self.assertEqual(call["endpoint"], "http://sghub.example/xet")
        self.assertEqual(call["files"][0].hash, "file-hash")
        self.assertEqual(call["files"][0].file_size, 16)

    def test_get_object_uses_regular_download_for_non_xet_physical_address(self):
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet)
        api.stat_object = lambda repository, ref, path: ObjectStats(
            path=path,
            path_type="object",
            physical_address="mem://objects/small.txt",
            checksum="checksum",
            size_bytes=5,
            mtime=0,
        )

        with patch.object(ObjectsApi, "get_object", return_value=b"small") as regular_download:
            data = api.get_object("repo", "main", "small.txt")

        self.assertEqual(data, b"small")
        regular_download.assert_called_once()
        self.assertEqual(hf_xet.download_calls, [])

    def test_get_object_skips_xet_for_range_reads(self):
        from surogate_hub_sdk.xet_objects_api import ObjectsApi

        hf_xet = FakeHfXet()
        api = self.api(hf_xet)
        stat_calls = []
        api.stat_object = lambda repository, ref, path: stat_calls.append((repository, ref, path))

        with patch.object(ObjectsApi, "get_object", return_value=b"range") as regular_download:
            data = api.get_object("repo", "main", "models/model.bin", range="bytes=0-1")

        self.assertEqual(data, b"range")
        regular_download.assert_called_once()
        self.assertEqual(stat_calls, [])
        self.assertEqual(hf_xet.download_calls, [])


if __name__ == "__main__":
    unittest.main()
