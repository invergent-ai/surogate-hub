import unittest
from types import SimpleNamespace

from surogate_hub_sdk.configuration import Configuration


class FakeHfXet:
    class PyXetDownloadInfo:
        def __init__(self, destination_path, hash, file_size):
            self.destination_path = destination_path
            self.hash = hash
            self.file_size = file_size

    def __init__(self):
        self.upload_calls = []
        self.download_calls = []

    def upload_files(
        self,
        file_paths,
        endpoint,
        token_info,
        token_refresher,
        progress_updater,
        repo_type,
        request_headers,
        sha256s,
        skip_sha256,
    ):
        self.upload_calls.append(
            {
                "file_paths": file_paths,
                "endpoint": endpoint,
                "token_info": token_info,
                "token_refresher": token_refresher,
                "progress_updater": progress_updater,
                "repo_type": repo_type,
                "request_headers": request_headers,
                "sha256s": sha256s,
                "skip_sha256": skip_sha256,
            }
        )
        return [SimpleNamespace(hash="file-hash", file_size=123)]

    def download_files(
        self,
        files,
        endpoint,
        token_info,
        token_refresher,
        progress_updater,
        request_headers,
    ):
        self.download_calls.append(
            {
                "files": files,
                "endpoint": endpoint,
                "token_info": token_info,
                "token_refresher": token_refresher,
                "progress_updater": progress_updater,
                "request_headers": request_headers,
            }
        )
        return [files[0].destination_path]


class FakeStagingApi:
    def __init__(self):
        self.calls = []

    def link_physical_address(self, repository, branch, path, staging_metadata, if_none_match=None, **kwargs):
        self.calls.append(
            {
                "repository": repository,
                "branch": branch,
                "path": path,
                "staging_metadata": staging_metadata,
                "if_none_match": if_none_match,
                "kwargs": kwargs,
            }
        )
        return SimpleNamespace(path=path)


class FakeObjectsApi:
    def __init__(self, physical_address="xet://file-hash", size_bytes=123):
        self.physical_address = physical_address
        self.size_bytes = size_bytes
        self.calls = []

    def stat_object(self, repository, ref, path):
        self.calls.append({"repository": repository, "ref": ref, "path": path})
        return SimpleNamespace(
            physical_address=self.physical_address,
            size_bytes=self.size_bytes,
        )


class FakeHubClient:
    def __init__(self):
        configuration = Configuration(
            host="http://sghub.example/api/v1",
            username="access",
            password="secret",
        )
        self._api = SimpleNamespace(configuration=configuration)
        self.staging_api = FakeStagingApi()
        self.objects_api = FakeObjectsApi()


class TestXETClient(unittest.TestCase):
    def test_hub_client_accepts_default_pool_threads_argument(self):
        from surogate_hub_sdk.client import HubClient

        configuration = Configuration(
            host="http://sghub.example/api/v1",
            username="access",
            password="secret",
        )

        client = HubClient(configuration=configuration)

        self.assertEqual(client._api.configuration.host, "http://sghub.example/api/v1")

    def test_upload_file_uploads_to_hf_xet_and_links_physical_address(self):
        from surogate_hub_sdk.xet import XETClient

        hf_xet = FakeHfXet()
        sghub = FakeHubClient()
        client = XETClient(
            sghub,
            hf_xet_module=hf_xet,
            token_info=("token", 4102444800),
            token_refresher=lambda: ("token", 4102444800),
        )

        result = client.upload_file(
            "repo",
            "main",
            "models/model.bin",
            "/tmp/model.bin",
            content_type="application/octet-stream",
            metadata={"k": "v"},
            if_none_match="*",
        )

        self.assertEqual(result.file_hash, "file-hash")
        self.assertEqual(hf_xet.upload_calls[0]["endpoint"], "http://sghub.example/xet")
        self.assertEqual(hf_xet.upload_calls[0]["file_paths"], ["/tmp/model.bin"])
        link = sghub.staging_api.calls[0]
        self.assertEqual(link["repository"], "repo")
        self.assertEqual(link["branch"], "main")
        self.assertEqual(link["path"], "models/model.bin")
        self.assertEqual(link["if_none_match"], "*")
        self.assertEqual(link["staging_metadata"].staging.physical_address, "xet://file-hash")
        self.assertEqual(link["staging_metadata"].checksum, "file-hash")
        self.assertEqual(link["staging_metadata"].size_bytes, 123)
        self.assertEqual(link["staging_metadata"].content_type, "application/octet-stream")
        self.assertEqual(link["staging_metadata"].user_metadata, {"k": "v"})

    def test_download_file_uses_hf_xet_for_xet_physical_address(self):
        from surogate_hub_sdk.xet import XETClient

        hf_xet = FakeHfXet()
        sghub = FakeHubClient()
        client = XETClient(
            sghub,
            hf_xet_module=hf_xet,
            token_info=("token", 4102444800),
            token_refresher=lambda: ("token", 4102444800),
        )

        result = client.download_file("repo", "main", "models/model.bin", "/tmp/out.bin")

        self.assertEqual(result, "/tmp/out.bin")
        self.assertEqual(sghub.objects_api.calls[0], {
            "repository": "repo",
            "ref": "main",
            "path": "models/model.bin",
        })
        call = hf_xet.download_calls[0]
        self.assertEqual(call["endpoint"], "http://sghub.example/xet")
        self.assertEqual(call["files"][0].destination_path, "/tmp/out.bin")
        self.assertEqual(call["files"][0].hash, "file-hash")
        self.assertEqual(call["files"][0].file_size, 123)


if __name__ == "__main__":
    unittest.main()
