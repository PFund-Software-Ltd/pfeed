from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.enums import Compression
    from pfeed.utils.file_path import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel

import pyarrow as pa
import pyarrow.fs as pa_fs

from pfeed._io.base_io import BaseIO, MetadataModelAsDict


class TableIO(BaseIO):
    IS_TABLE_FORMAT: bool = True

    def __init__(
        self,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        storage_options: dict | None = None,
        compression: Compression | str | None=None, 
        **kwargs,
    ):
        super().__init__(filesystem=filesystem, storage_options=storage_options, compression=None, **kwargs)
    
    # HACK: delta-rs, lancedb doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
    def write_metadata(self, file_path: FilePath, metadata: BaseMetadataModel):
        import pandas as pd
        empty_df_with_metadata = pd.DataFrame()
        table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
        metadata_file_path = file_path / self.METADATA_FILENAME
        self._write_pyarrow_table_with_metadata(table, metadata_file_path, metadata=metadata)
    
    def read_metadata(self, file_paths: FilePath | list[FilePath]) -> dict[FilePath, MetadataModelAsDict]:
        if isinstance(file_paths, list):
            assert len(file_paths) == 1, 'table format should have exactly one file path'
            table_path = file_paths[0]
        else:
            table_path = file_paths
        if table_path.name != self.METADATA_FILENAME:
            file_paths = [table_path / self.METADATA_FILENAME]
        else:
            file_paths = [table_path]
        return super().read_metadata(file_paths=file_paths)
