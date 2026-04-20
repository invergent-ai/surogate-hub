"""fsspec filesystem for Surogate Hub.

Hand-written module (not generated). See ``.openapi-generator-ignore``.
"""

from surogate_hub_sdk.fs.filesystem import (
    PROTOCOL,
    PROTOCOL_SCHEME,
    SurogateHubFile,
    SurogateHubFileSystem,
    register,
)

register()

__all__ = [
    "PROTOCOL",
    "PROTOCOL_SCHEME",
    "SurogateHubFile",
    "SurogateHubFileSystem",
]
