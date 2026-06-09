# pyright: reportUnknownMemberType=false, reportArgumentType=false, reportAttributeAccessIssue=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, assert_never

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from pfeed._io.base_io import BaseIO, MetadataDict
    from pfeed._sinks.base_sink import BaseSink
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.database_storage import DatabaseURI

    IOClassName: TypeAlias = str

from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict

from pfeed._io.database_io import DBPath
from pfeed._io.table_io import TablePath
from pfeed.enums import DataLayer, DataSource, IOType
from pfeed.utils.file_path import FilePath

SourcePath: TypeAlias = FilePath | TablePath | DBPath


class BaseDataMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    data_source: DataSource
    data_origin: str = ""


class BaseDataHandler(ABC):
    metadata_class: ClassVar[type[BaseDataMetadata]]
    PARTITION_COLUMNS: ClassVar[list[str]] = []
    IO_USING_PARTITION_COLUMNS: ClassVar[set[IOClassName]] = set()

    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer,
        data_domain: str,
        data_model: BaseDataModel,
        io: BaseIO,
        sink: BaseSink | None = None,
    ):
        self._data_path = data_path
        self._data_layer = data_layer
        self._data_domain = data_domain
        self._data_model = data_model
        self._io: BaseIO = io
        self._sink: BaseSink | None = sink
        self._io_type: IOType = self._get_io_type()
        self._file_paths: list[FilePath] = []
        self._table_path: TablePath | None = (
            self._create_table_path() if self._io_type == IOType.TABLE else None
        )
        self._db_path: DBPath | None = (
            self._create_db_path() if self._io_type == IOType.DATABASE else None
        )
        self._sink_path: SourcePath | None = (
            self._create_sink_path() if self._sink else None
        )
        if self._sink:
            self.sink.with_schema(schema=self._build_streaming_schema())

    @abstractmethod
    def write_batch(self, data: IntoFrame | bytes, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def read(self, **kwargs: Any) -> Any | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: Any) -> Any:
        pass

    @abstractmethod
    def _create_file_path(self, *args: Any, **kwargs: Any) -> FilePath:
        pass

    @abstractmethod
    def _create_table_path(self, *args: Any, **kwargs: Any) -> TablePath:
        pass

    @abstractmethod
    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        pass

    @abstractmethod
    def _create_metadata(self, *args: Any, **kwargs: Any) -> BaseDataMetadata:
        pass

    @property
    def io(self) -> BaseIO:
        return self._io

    @property
    def sink(self) -> BaseSink:
        if self._sink is None:
            raise ValueError("sink is not initialized")
        return self._sink

    def _get_file_extension(self) -> str:
        if self.io.FILE_EXTENSION is None:
            raise ValueError(
                f"{self._io.__class__.__name__} does not have a file extension"
            )
        return self.io.FILE_EXTENSION

    def _get_io_type(self) -> IOType:
        if self._io.is_file_io():
            return IOType.FILE
        elif self._io.is_table_io():
            return IOType.TABLE
        elif self._io.is_database_io():
            return IOType.DATABASE
        else:
            raise ValueError(f"Unsupported IO type: {self._io}")

    def read_metadata(self) -> dict[SourcePath, BaseDataMetadata]:
        match self._io_type:
            case IOType.FILE:
                source_paths = self._file_paths
            case IOType.TABLE:
                source_paths = self._table_path
            case IOType.DATABASE:
                source_paths = self._db_path
            case _:
                assert_never(self._io_type)
        metadata_dict: dict[SourcePath, MetadataDict] = self.io.read_metadata(
            source_paths
        )
        Metadata = self.metadata_class
        metadata = {
            source_path: Metadata(**metadata_value)
            for source_path, metadata_value in metadata_dict.items()
        }
        return metadata

    def find_missing_source_paths(self):
        match self._io_type:
            case IOType.FILE:
                missing_source_paths = [
                    fp for fp in self._file_paths if not self.io.exists(fp)
                ]
            case (IOType.TABLE | IOType.DATABASE) as io_type:
                source_path = (
                    self._table_path if io_type == IOType.TABLE else self._db_path
                )
                assert source_path is not None, (
                    f"source_path is not set for {self.io.name}"
                )
                missing_source_paths = (
                    [source_path] if not self.io.exists(source_path) else []
                )
            case _:
                assert_never(self._io_type)
        return missing_source_paths

    def _requires_partitioning(self) -> bool:
        # REVIEW: only deltalake requires partitioning
        return (
            self.io.SUPPORTS_PARTITIONING
            and self.io.name in self.IO_USING_PARTITION_COLUMNS
        )
