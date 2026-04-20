"""Post-commit stats worker for Surogate Hub dataset repositories."""

from surogate_hub_worker import fs  # register 'shub://' fsspec protocol

__all__ = ["fs"]
__version__ = "0.1.0"
