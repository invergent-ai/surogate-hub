import json
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

from surogate_hub_sdk.api.objects_api import ObjectsApi
from surogate_hub_sdk.api.staging_api import StagingApi
from surogate_hub_sdk.models.staging_location import StagingLocation
from surogate_hub_sdk.models.staging_metadata import StagingMetadata


TokenInfo = Tuple[str, int]


class XetObjectsApi(ObjectsApi):
    def __init__(
        self,
        api_client=None,
        xet_endpoint: Optional[str] = None,
        hf_xet_module=None,
        token_info: Optional[TokenInfo] = None,
        token_refresher=None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(api_client)
        self._staging_api = StagingApi(self.api_client)
        self.configuration = self.api_client.configuration
        self.xet_endpoint = (xet_endpoint or self._derive_xet_endpoint(self.configuration.host)).rstrip("/")
        self.hf_xet = hf_xet_module
        self._token_info = token_info
        self._token_refresher = token_refresher
        self.request_headers = request_headers

    def upload_object(
        self,
        repository,
        branch,
        path,
        if_none_match=None,
        storage_class=None,
        force=None,
        content=None,
        _request_timeout=None,
        _request_auth=None,
        _content_type=None,
        _headers=None,
        _host_index=0,
    ):
        xet_content = self._xet_upload_content(content)
        if xet_content is None or storage_class is not None or _request_auth is not None:
            return super().upload_object(
                repository,
                branch,
                path,
                if_none_match=if_none_match,
                storage_class=storage_class,
                force=force,
                content=content,
                _request_timeout=_request_timeout,
                _request_auth=_request_auth,
                _content_type=_content_type,
                _headers=_headers,
                _host_index=_host_index,
            )

        local_path, cleanup_path = xet_content
        try:
            hf_xet = self._hf_xet()
            upload_info = hf_xet.upload_files(
                [local_path],
                self.xet_endpoint,
                self._get_token_info(),
                self._refresh_token,
                None,
                None,
                _headers or self.request_headers,
                None,
                False,
            )[0]
            staging_metadata = StagingMetadata(
                staging=StagingLocation(physical_address="xet://" + upload_info.hash),
                checksum=upload_info.hash,
                size_bytes=upload_info.file_size,
                content_type=_content_type,
                force=bool(force),
            )
            return self._staging_api.link_physical_address(
                repository,
                branch,
                path,
                staging_metadata,
                if_none_match=if_none_match,
                _request_timeout=_request_timeout,
                _headers=_headers,
                _host_index=_host_index,
            )
        finally:
            if cleanup_path is not None:
                try:
                    os.unlink(cleanup_path)
                except FileNotFoundError:
                    pass

    def get_object(
        self,
        repository,
        ref,
        path,
        range=None,
        if_none_match=None,
        presign=None,
        _request_timeout=None,
        _request_auth=None,
        _content_type=None,
        _headers=None,
        _host_index=0,
    ):
        if range is not None or if_none_match is not None or presign or _request_auth is not None:
            return super().get_object(
                repository,
                ref,
                path,
                range=range,
                if_none_match=if_none_match,
                presign=presign,
                _request_timeout=_request_timeout,
                _request_auth=_request_auth,
                _content_type=_content_type,
                _headers=_headers,
                _host_index=_host_index,
            )

        stats = self.stat_object(repository, ref, path)
        physical_address = stats.physical_address
        if not physical_address.startswith("xet://"):
            return super().get_object(
                repository,
                ref,
                path,
                range=range,
                if_none_match=if_none_match,
                presign=presign,
                _request_timeout=_request_timeout,
                _request_auth=_request_auth,
                _content_type=_content_type,
                _headers=_headers,
                _host_index=_host_index,
            )

        file_hash = physical_address[len("xet://"):]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp_path = tmp.name
        tmp.close()
        try:
            downloads = [
                self._hf_xet().PyXetDownloadInfo(
                    tmp_path,
                    file_hash,
                    stats.size_bytes,
                )
            ]
            self._hf_xet().download_files(
                downloads,
                self.xet_endpoint,
                self._get_token_info(),
                self._refresh_token,
                None,
                _headers or self.request_headers,
            )
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass

    def _xet_upload_content(self, content):
        if content is None:
            return None
        if isinstance(content, str):
            return content, None
        if isinstance(content, (bytes, bytearray)):
            return self._spool_bytes(bytes(content))
        if isinstance(content, tuple) and len(content) == 2 and isinstance(content[1], (bytes, bytearray)):
            return self._spool_bytes(bytes(content[1]))
        return None

    @staticmethod
    def _spool_bytes(data: bytes):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            tmp.write(data)
            return tmp.name, tmp.name
        finally:
            tmp.close()

    def _hf_xet(self):
        if self.hf_xet is None:
            self.hf_xet = self._load_hf_xet()
        return self.hf_xet

    def _get_token_info(self) -> TokenInfo:
        if self._token_info is None:
            self._token_info = self._mint_token()
        return self._token_info

    def _refresh_token(self) -> TokenInfo:
        if self._token_refresher is not None:
            self._token_info = self._token_refresher()
            return self._token_info

        token, _ = self._get_token_info()
        request = urllib.request.Request(self.xet_endpoint + "/v1/token/refresh", method="GET")
        request.add_header("Authorization", "Bearer " + token)
        try:
            self._token_info = self._read_token_response(request)
        except urllib.error.HTTPError:
            self._token_info = self._mint_token()
        return self._token_info

    def _mint_token(self) -> TokenInfo:
        request = urllib.request.Request(self.xet_endpoint + "/v1/token", method="POST")
        request.add_header("Authorization", self._lakefs_auth_header())
        return self._read_token_response(request)

    @staticmethod
    def _read_token_response(request: urllib.request.Request) -> TokenInfo:
        with urllib.request.urlopen(request) as response:
            data = json.loads(response.read().decode("utf-8"))
        return data["access_token"], int(data["exp"])

    def _lakefs_auth_header(self) -> str:
        if self.configuration.username is not None and self.configuration.password is not None:
            return self.configuration.get_basic_auth_token()
        if self.configuration.access_token is not None:
            return "Bearer " + self.configuration.access_token
        raise ValueError("XET token minting requires basic credentials or an access token")

    @staticmethod
    def _derive_xet_endpoint(api_endpoint: str) -> str:
        parsed = urllib.parse.urlsplit(api_endpoint)
        path = parsed.path.rstrip("/")
        if path == "/api/v1":
            root_path = ""
        elif path.endswith("/api/v1"):
            root_path = path[: -len("/api/v1")]
        else:
            root_path = path
        xet_path = root_path.rstrip("/") + "/xet"
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, xet_path, "", ""))

    @staticmethod
    def _load_hf_xet():
        try:
            import hf_xet
        except ImportError as exc:
            raise ImportError(
                "XET support requires hf_xet==1.4.3. Reinstall surogate-hub-sdk with its required dependencies."
            ) from exc
        return hf_xet
