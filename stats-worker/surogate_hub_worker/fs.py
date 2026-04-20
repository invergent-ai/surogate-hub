"""Re-export of the SDK's fsspec filesystem so worker imports stay stable.

The filesystem itself lives in :mod:`surogate_hub_sdk.fs` — the worker
was only its first consumer. :class:`surogate_hub_sdk.parquet.ParquetQuery`
also uses it as a fallback when the hub's blockstore can't presign.
"""

from surogate_hub_sdk.fs import (
    PROTOCOL,
    PROTOCOL_SCHEME,
    SurogateHubFile,
    SurogateHubFileSystem,
)

__all__ = [
    "PROTOCOL",
    "PROTOCOL_SCHEME",
    "SurogateHubFile",
    "SurogateHubFileSystem",
]
