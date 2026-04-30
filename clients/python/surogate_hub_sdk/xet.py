import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from surogate_hub_sdk.models.staging_location import StagingLocation
from surogate_hub_sdk.models.staging_metadata import StagingMetadata


TokenInfo = Tuple[str, int]


@dataclass
class XETUploadResult:
    file_hash: str
    size_bytes: int
    object_stats: Any
    upload_info: Any


class XETClient:
    def __init__(
        self,
        lakefs_client,
        xet_endpoint: Optional[str] = None,
        hf_xet_module=None,
        token_info: Optional[TokenInfo] = None,
        token_refresher: Optional[Callable[[], TokenInfo]] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ):
        self.lakefs_client = lakefs_client
        self.configuration = lakefs_client._api.configuration
        self.xet_endpoint = (xet_endpoint or self._derive_xet_endpoint(self.configuration.host)).rstrip("/")
        self.hf_xet = hf_xet_module or self._load_hf_xet()
        self._token_info = token_info
        self._token_refresher = token_refresher
        self.request_headers = request_headers

    def upload_file(
        self,
        repository: str,
        branch: str,
        path: str,
        local_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        if_none_match: Optional[str] = None,
        progress_updater: Optional[Callable[[int], None]] = None,
        sha256: Optional[str] = None,
        skip_sha256: bool = False,
    ) -> XETUploadResult:
        sha256s = [sha256] if sha256 is not None else None
        upload_info = self.hf_xet.upload_files(
            [str(local_path)],
            self.xet_endpoint,
            self._get_token_info(),
            self._refresh_token,
            progress_updater,
            None,
            self.request_headers,
            sha256s,
            skip_sha256,
        )[0]

        file_hash = upload_info.hash
        size_bytes = upload_info.file_size
        staging_metadata = StagingMetadata(
            staging=StagingLocation(physical_address="xet://" + file_hash),
            checksum=file_hash,
            size_bytes=size_bytes,
            user_metadata=metadata,
            content_type=content_type,
        )
        object_stats = self.lakefs_client.staging_api.link_physical_address(
            repository,
            branch,
            path,
            staging_metadata,
            if_none_match=if_none_match,
        )
        return XETUploadResult(
            file_hash=file_hash,
            size_bytes=size_bytes,
            object_stats=object_stats,
            upload_info=upload_info,
        )

    def download_file(
        self,
        repository: str,
        ref: str,
        path: str,
        destination_path: str,
        progress_updater: Optional[Callable[[int], None]] = None,
    ) -> str:
        stats = self.lakefs_client.objects_api.stat_object(repository, ref, path)
        physical_address = stats.physical_address
        if not physical_address.startswith("xet://"):
            raise ValueError("object is not XET-backed")

        file_hash = physical_address[len("xet://"):]
        downloads = [
            self.hf_xet.PyXetDownloadInfo(
                str(destination_path),
                file_hash,
                stats.size_bytes,
            )
        ]
        progress = [progress_updater] if progress_updater is not None else None
        return self.hf_xet.download_files(
            downloads,
            self.xet_endpoint,
            self._get_token_info(),
            self._refresh_token,
            progress,
            self.request_headers,
        )[0]

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
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, xet_path, "", "")
        )

    @staticmethod
    def _load_hf_xet():
        try:
            import hf_xet
        except ImportError as exc:
            raise ImportError(
                "XET support requires hf_xet==1.4.3. Reinstall surogate-hub-sdk with its required dependencies."
            ) from exc
        return hf_xet
