from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, assert_never, cast

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from pfeed._io.database_io import DBPath
    from pfeed._io.file_io import FileIO
    from pfeed._io.table_io import TablePath
    from pfeed.sources.pfund.component_data_model import PFundComponentDataModel

import polars as pl
from pydantic import Field

from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.enums import DataLayer, DataSource, DataTool
from pfeed.utils.file_path import FilePath
from pfund.enums import ArtifactType, RunMode
from pfund.typing import ComponentName


class ComponentDataMetadata(BaseDataMetadata):
    size_bytes: int | None = None  # only data of type bytes has a size

    run_mode: RunMode
    signature: tuple[tuple[Any, ...], dict[str, Any]]  # (args, kwargs)
    config: dict[str, Any]
    params: dict[str, Any]
    settings: dict[str, Any]
    datas: list[dict[str, Any]]
    strategies: list[ComponentName] = Field(default_factory=list)
    models: list[ComponentName] = Field(default_factory=list)
    features: list[ComponentName] = Field(default_factory=list)
    indicators: list[ComponentName] = Field(default_factory=list)


# TODO (mtflow): add tags to components, parent/child relationships/lineage (component in refinement is from which experiment?)
"""
pfund_data_path/
    # TODO: add more details like pfund version, pfeed version, etc.
    # mtflow.duckdb looks sth like this:
    # templates (list[dict]): e.g. {'name': 'pfund_official/backtest_template', 'version': '0.0.1'}
    # run_id  group run_name python_version	strategy_version	mtflow_version	artifact_path	sharpe	max_dd	templates	created_at
    # run_001	...  ...    3.11.14	        0.1.0	3	artifacts/run_001/	1.2	-0.08	{"window": 20}	...
    # run_002	...  ...    3.11.14	        0.1.0	3	artifacts/run_002/	1.5	-0.06	{"window": 50}	...
    # - component signature (includes __version__)
    # (chosen components from registry, deployment metadata, live trading tracking)
    mtflow.duckdb  (components lifecycle tracking, e.g. component status (retired?), metrics in different envs)
    registry/  (focuses on components lifecycle)
        {component_type}/
            {component_class_name}/
                {version}/  (if registered)
                    metadata.json
                    artifacts, e.g. strategy.py, model.pkl, delta table etc.
    runs/
        env=BACKTEST or SANDBOX or PAPER or LIVE/
            {project_name}/
                trackio.db  (optional)
                optuna.db  (optional for BACKTEST env)
                run_001/
                    pfund.duckdb  (backtest results per chunk? TBD) (engine states, e.g. orders, positions, trades, etc.)
                    artifacts/
                        {component_type}/
                            {unique_component_name}/
                                metadata.json
                                artifacts, e.g. strategy.py, strategy_{version}.py (when registered) model.pkl, delta table etc.
"""


class ComponentDataHandler(BaseDataHandler):
    """Persists pfund component artifacts on file-based storage.

    The artifact arrives already in its owner's currency — opaque bytes for
    model/source (serialized by pfund), a frame for `data` — so this handler does
    no (de)serialization itself: it just routes the payload to the IO at a path
    derived from the component's identity. With the blob path (IOFormat.BLOB -> FileIO)
    the payload is bytes; the IO writes/reads them verbatim.
    """

    _data_model: PFundComponentDataModel
    metadata_class: ClassVar[type[ComponentDataMetadata]] = ComponentDataMetadata

    def __init__(
        self,
        data_path: FilePath,
        data_layer: DataLayer,  # not in use
        data_domain: str,  # not in use
        data_model: PFundComponentDataModel,
        io: FileIO,
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            data_model=data_model,
            io=io,
        )
        self._file_paths = [self._create_file_path()]

    @property
    def file_path(self) -> FilePath:
        return self._file_paths[0]

    def _create_file_path(self) -> FilePath:
        artifact = self._data_model
        artifact_dir = (
            cast(FilePath, self._data_path)
            / f"env={artifact.env}"
            / artifact.project_name.lower()
            / artifact.run_name.lower()
            / "artifacts"
            / artifact.component_type.to_plural()
            / artifact.component_name  # NOTE: unique
        )
        match artifact.artifact_type:
            case ArtifactType.source:
                from pfeed.sources.pfund.component_data_model import SourceArtifact

                return artifact_dir / cast(SourceArtifact, artifact).filename
            case ArtifactType.model:
                # no original filename (the bytes come from component.dump()) — name it
                # by the unique component_name so read() reconstructs the same path.
                return artifact_dir / f"model{artifact.extension}"
            case ArtifactType.data:
                # .delta is a deltalake directory, not a file; same deterministic naming.
                return artifact_dir / f"data{artifact.extension}"
            case _:
                assert_never(artifact.artifact_type)

    def _create_table_path(self, *args: Any, **kwargs: Any) -> TablePath:
        raise NotImplementedError

    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        raise NotImplementedError

    def write_batch(self, data: IntoFrame | bytes) -> None:
        file_path = self._create_file_path()
        self._file_paths = [file_path]
        with self.io:
            if isinstance(data, bytes):
                # source (.py) and model weights (.safetensors / .joblib) arrive as
                # opaque bytes pfund already serialized; FileIO writes them verbatim.
                self.io.write(data, file_path)
                metadata = self._create_metadata(size_bytes=len(data))
                self.io.write_metadata(metadata, file_path)
            else:  # data artifact writing to deltalake
                from pfeed._etl.base import convert_dataframe

                lf = cast(pl.LazyFrame, convert_dataframe(data, DataTool.polars))
                self.io.write(lf.collect().to_arrow(), file_path)
                self.io.write_metadata(file_path, metadata=self._create_metadata())

    def read(self) -> pl.LazyFrame | bytes:
        return self.io.read(file_paths=self._file_paths)

    # REVIEW: no validation, not in use
    def _validate_schema(self, df: pl.LazyFrame) -> Any:
        return df

    def _create_metadata(self, size_bytes: int | None = None) -> ComponentDataMetadata:
        artifact = self._data_model
        return ComponentDataMetadata(
            data_source=DataSource[artifact.data_source.name],
            data_origin=artifact.data_origin,
            size_bytes=size_bytes,
            run_mode=RunMode[artifact.run_mode],
            signature=artifact.signature,
            config=artifact.config,
            params=artifact.params,
            settings=artifact.settings,
            datas=artifact.datas,
            strategies=artifact.strategies,
            models=artifact.models,
            features=artifact.features,
            indicators=artifact.indicators,
        )
