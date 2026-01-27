from __future__ import annotations
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    pass

import os

import pyarrow.fs as pa_fs

from pfeed.storages.file_based_storage import FileBasedStorage
from pfeed.enums import DataLayer


class HuggingFaceStorage(FileBasedStorage):
    """Storage backend for HuggingFace Hub datasets.

    Uses HuggingFace's HfFileSystem wrapped as a PyArrow filesystem.

    Example:
        # Direct instantiation
        storage = HuggingFaceStorage(
            data_path="hf://datasets/pfund-ai/market-data",
        )
    """

    def __init__(
        self,
        data_path: str,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str | Literal['MARKET_DATA', 'NEWS_DATA'] = 'MARKET_DATA',
        storage_options: dict | None = None,
    ):
        """
        Args:
            data_path: Full HuggingFace path with hf:// prefix
                (e.g., "hf://datasets/pfund-ai/market-data").
            data_layer: Data layer to store the data
            data_domain: Data domain of the data
            storage_options: Storage options. Can include:
                - 'token': HuggingFace API token (defaults to HF_TOKEN env var)
        """
        if not data_path.startswith("hf://"):
            data_path = f"hf://{data_path}"

        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )
        
        # Ensure storage_options has token
        if "token" not in self.storage_options:
            token = os.getenv("HF_TOKEN")
            if not token:
                raise ValueError(
                    "HuggingFace token not found. "
                    "Set HF_TOKEN environment variable or pass token in storage_options."
                )
            self.storage_options["token"] = token

    def get_filesystem(self) -> pa_fs.FileSystem:
        from huggingface_hub import HfFileSystem

        hf_fs = HfFileSystem(token=self.storage_options.get("token"))
        return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(hf_fs))
