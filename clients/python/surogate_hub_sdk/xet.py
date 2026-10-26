from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from surogate_hub_sdk.models.staging_location import StagingLocation
from surogate_hub_sdk.models.staging_metadata import StagingMetadata
from surogate_hub_sdk.repository import split_repository_id
from surogate_hub_sdk.xet_objects_api import XetObjectsApi


@dataclass
class XETUploadResult:
    file_hash: str
    size_bytes: int
    object_stats: Any
    upload_info: Any


class XETClient:
    def __init__(
        self,
        hub_client,
        xet_endpoint: Optional[str] = None,
        hf_xet_module=None,
        request_headers: Optional[Dict[str, str]] = None,
    ):
        self.hub_client = hub_client
        self._objects = XetObjectsApi(
            hub_client._api,
            xet_endpoint=xet_endpoint,
            hf_xet_module=hf_xet_module,
            request_headers=request_headers,
        )
        self.xet_endpoint = self._objects.xet_endpoint
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
        user, repo_name = split_repository_id(repository)
        token_info, refresh_token = self._objects._xet_token(user, repo_name, branch, path, "write")
        upload_info = self._objects._hf_xet().upload_files(
            [str(local_path)],
            self.xet_endpoint,
            token_info,
            refresh_token,
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
        object_stats = self.hub_client.staging_api.link_physical_address(
            user=user,
            repository=repo_name,
            branch=branch,
            path=path,
            staging_metadata=staging_metadata,
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
        user, repo_name = split_repository_id(repository)
        stats = self.hub_client.objects_api.stat_object(
            user=user, repository=repo_name, ref=ref, path=path,
        )
        physical_address = stats.physical_address
        if not physical_address.startswith("xet://"):
            raise ValueError("object is not XET-backed")

        file_hash = physical_address[len("xet://"):]
        token_info, refresh_token = self._objects._xet_token(user, repo_name, ref, path, "read")
        hf_xet = self._objects._hf_xet()
        downloads = [
            hf_xet.PyXetDownloadInfo(
                str(destination_path),
                file_hash,
                stats.size_bytes,
            )
        ]
        progress = [progress_updater] if progress_updater is not None else None
        return hf_xet.download_files(
            downloads,
            self.xet_endpoint,
            token_info,
            refresh_token,
            progress,
            self.request_headers,
        )[0]
