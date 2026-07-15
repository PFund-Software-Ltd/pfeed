from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, assert_never, cast

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from pfeed._io.database_io import DBPath
    from pfeed._io.file_io import FileIO
    from pfeed._sinks.base_sink import BaseSink
    from pfeed.sources.pfund.component_data_model import PFundComponentDataModel

import polars as pl

from pfeed._io.table_io import TablePath
from pfeed.data_handlers.base_data_handler import BaseDataHandler
from pfeed.enums import DataLayer, DataTool, IOType
from pfeed.sources.pfund.component_metadata import PFundComponentDataMetadata
from pfeed.utils.file_path import FilePath
from pfund.enums import ArtifactType, ComponentType, Environment

# TODO (mtflow): add tags to components, parent/child relationships/lineage (component in refinement is from which experiment?)
"""
pfund_data_path/
    # TODO: add more details like pfund version, pfeed version, etc.
    # mtflow.db looks sth like this:
    # templates (list[dict]): e.g. {'name': 'pfund_official/backtest_template', 'version': '0.0.1'}
    # run_id  group run_name python_version	strategy_version	mtflow_version	artifact_path	sharpe	max_dd	templates	created_at
    # run_001	...  ...    3.11.14	        0.1.0	3	artifacts/run_001/	1.2	-0.08	{"window": 20}	...
    # run_002	...  ...    3.11.14	        0.1.0	3	artifacts/run_002/	1.5	-0.06	{"window": 50}	...
    # - component signature (includes __version__)
    # (chosen components from registry, deployment metadata, live trading tracking)
    mtflow.db  (components lifecycle tracking, e.g. component status (retired?), metrics in different envs)
    registry/  (focuses on components lifecycle)
        {component_type}/
            {component_class_name}/
                {version}/  (if registered)
                    metadata.json
                    artifacts, e.g.
                    - strategy.py
                    - trained model (e.g. .safetensors)
                    - trading_df.delta (delta table)
                    - etc.
    runs/
        env=BACKTEST or SANDBOX or PAPER or LIVE/
            {project_name}/
                trackio.db  (optional)
                optuna.db  (optional for BACKTEST env)
                run_001/
                    (engine states (e.g. used to match back client order ids),
                        also used to host a fake server in SANDBOX trading)
                    pfund.db
                    artifacts/
                        {component_type}/
                            {unique_component_name}/
                                metadata.json
                                artifacts, e.g.
                                - strategy.py
                                - trained model (e.g. .safetensors)
                                - trading_df.delta (delta table)
                                - etc.
"""


class PFundComponentDataHandler(BaseDataHandler):
    """Persists pfund component artifacts on file-based storage.

    The artifact arrives already in its owner's currency — opaque bytes for
    model/source (serialized by pfund), a frame for `data` — so this handler does
    no (de)serialization itself: it just routes the payload to the IO at a path
    derived from the component's identity. With the blob path (IOFormat.BLOB -> FileIO)
    the payload is bytes; the IO writes/reads them verbatim.
    """

    _data_model: PFundComponentDataModel
    Metadata: ClassVar[type[PFundComponentDataMetadata]] = PFundComponentDataMetadata

    def __init__(
        self,
        data_path: FilePath,
        data_layer: DataLayer,  # not in use
        data_domain: str,  # not in use
        data_model: PFundComponentDataModel,
        io: FileIO,
        sink: BaseSink | None = None,
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            data_model=data_model,
            io=io,
            sink=sink,
        )
        if self._io_type == IOType.FILE:
            self._file_paths = [self._create_file_path()]
        elif self._io_type == IOType.TABLE and self._table_path:
            artifact = self._data_model
            if artifact.artifact_type == ArtifactType.data:
                self._table_path /= f"trading_df{artifact.extension}"

    @property
    def file_path(self) -> FilePath:
        return self._file_paths[0]

    def _create_file_path(self) -> FilePath:
        artifact = self._data_model
        artifact_dir = self._create_table_path()
        match artifact.artifact_type:
            case ArtifactType.source:
                from pfeed.sources.pfund.component_data_model import SourceArtifact

                return artifact_dir / cast(SourceArtifact, artifact).filename
            case ArtifactType.model:
                return artifact_dir / f"model{artifact.extension}"
            case ArtifactType.checkpoint:
                from pfeed.sources.pfund.component_data_model import CheckpointArtifact

                return (
                    artifact_dir
                    / "checkpoints"
                    / f"step={cast(CheckpointArtifact, artifact).step}"
                    / f"checkpoint{artifact.extension}"
                )
            case ArtifactType.data:
                raise ValueError("data artifacts use a table path, not a file path")
            case _:
                assert_never(artifact.artifact_type)

    def _create_table_path(self) -> TablePath:
        from pfund.engines.base_engine import BaseEngine

        artifact = self._data_model
        run_path = BaseEngine._create_run_path(
            data_path=cast(FilePath, self._data_path),
            env=Environment[artifact.env],
            project_name=artifact.project_name,
            run_name=artifact.run_name,
        )
        return TablePath(
            run_path
            / "artifacts"
            / ComponentType[artifact.component_type].to_plural()
            / artifact.component_id
        )

    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        raise NotImplementedError

    def write_batch(self, data: IntoFrame | bytes) -> None:
        with self.io:
            if isinstance(data, bytes):
                # source (.py) and model weights (.safetensors / .joblib) arrive as
                # opaque bytes pfund already serialized; FileIO writes them verbatim.
                self.io.write(data, self.file_path)
                if self._data_model.artifact_type == ArtifactType.source:
                    self.io.write_metadata(
                        self.file_path, metadata=self._create_metadata()
                    )
            else:  # data artifact writing to deltalake
                from pfeed._etl.base import convert_dataframe

                table_path = self._table_path
                assert table_path is not None, "table path is not initialized"
                lf = cast(pl.LazyFrame, convert_dataframe(data, DataTool.polars))
                self.io.write(lf.collect().to_arrow(), table_path)

    def read(self) -> pl.LazyFrame | bytes | None:
        match self._io_type:
            case IOType.FILE:
                return self.io.read(file_paths=self._file_paths)
            case IOType.TABLE:
                table_path = self._table_path
                assert table_path is not None, "table path is not initialized"
                return self.io.read(table_path)
            case IOType.DATABASE:
                raise NotImplementedError(
                    f"{self.__class__.__name__} does not support database IO"
                )
            case _:
                assert_never(self._io_type)

    # REVIEW: no validation, not in use
    def _validate_schema(self, df: pl.LazyFrame) -> Any:
        return df

    def _create_metadata(self) -> PFundComponentDataMetadata:
        metadata = self._data_model.metadata
        if metadata is None:
            raise ValueError("component metadata is required when writing an artifact")
        return metadata
