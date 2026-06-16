# pyright: reportUnusedParameter=false, reportUnknownMemberType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self, assert_never, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from narwhals.typing import IntoFrame

    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.result import DataFlowResult, RunResult
    from pfeed.sources.pfund.component_data_model import PFundComponentDataModel
    from pfeed.sources.pfund.requests import (
        PFundComponentFeedDownloadRequest,
        PFundComponentFeedRetrieveRequest,
    )
    from pfund.enums import ComponentType
    from pfund.typing import Component

    PFundComponentFeedRequest = (
        PFundComponentFeedDownloadRequest | PFundComponentFeedRetrieveRequest
    )

from pathlib import Path

import polars as pl
from pfund_kit.style import RichColor, TextStyle

from pfeed._io.io_config import IOConfig
from pfeed.config import setup_logging
from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.enums import DataCategory, DataStorage, DataTool, IOFormat
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.storages.storage_config import StorageConfig
from pfund.engines.engine_context import EngineContext
from pfund.enums import ArtifactType, Environment


class PFundComponentFeed(PFundMixin, BaseFeed):
    """Feed over a pfund component's artifacts (data, model, source).

    Architecture (see the design discussion):
      - `download(artifact_type)` is the EXTRACT: it pulls the artifact out of the
        component.
      - `load(...)` is the LOAD: it reuses BaseFeed's storage machinery to write the
        downloaded artifact. The IO format is chosen from the artifact's extension —
        `.delta` -> DeltaLakeIO (frames, pfeed's native currency), everything else ->
        FileIO via IOFormat.BLOB (opaque bytes; pfund owns the serialization).
      - `retrieve(...)` is the read-back twin of `download()`+`load()`: same identity,
        but the extract source is pfeed's STORAGE instead of the live component. No
        serialization happens — FileIO/DeltaLakeIO hand the persisted bytes/frame back.
    """

    # not really used (the concrete model varies per artifact_type) but satisfies the
    # BaseFeed annotation; the real per-request model is built in _create_data_model_from_request.
    data_model_class: ClassVar[type[BaseDataModel]] = BaseDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.COMPONENT_DATA

    def __init__(
        self,
        component: Component | None = None,
        pipeline_mode: bool = False,
        num_workers: int | None = None,
    ):
        """
        Args:
            component: the component this feed operates on. Optional rather than
                required because the client builds the feed eagerly, before any
                component exists (`PFund._create_feeds`), then points it via
                `with_component()`. Direct users can pass it up front:
                `PFundComponentFeed(component)`.
            pipeline_mode: see BaseFeed. The feed already defers everything to an
                explicit `download()/load()/run()`, so this just composes with pfeed's
                pipeline machinery.
            num_workers: see BaseFeed. NOTE: only sound for the read path (`retrieve`).
                On `download`, the extract closure is a bound method, so shipping a
                dataflow to a Ray worker pickles the whole feed — including the live
                component. That contradicts the "component is local, only bytes move"
                principle and breaks on unpicklable components, so prefer leaving it
                None for downloads.
        """
        super().__init__(pipeline_mode=pipeline_mode, num_workers=num_workers)
        self._component: Component | None = component

    def with_component(self, component: Component) -> Self:
        """Point the feed at a component (fluent builder setter).

        Conceptually the same as storage's `with_data_model()` — it sets the one
        subject this feed operates on and returns self for chaining, so switching
        components is just `feed.with_component(other).retrieve(...)`.
        """
        self._component = component
        return self

    @property
    def component(self) -> Component:
        assert self._component is not None, "component is not set"
        return self._component

    @property
    def component_type(self) -> ComponentType:
        return self.component.component_type

    @property
    def component_source_file(self) -> Path:
        """Locate the component's original `.py` source file on disk."""
        import inspect

        source_file = inspect.getsourcefile(type(self.component))
        if source_file is None:
            raise ValueError(
                f"cannot locate source file for {type(self.component).__name__}"
            )
        return Path(source_file)

    def _append_request(self, request: PFundComponentFeedRequest) -> None:
        # A component feed has exactly one purpose per run: download/retrieve ONE
        # artifact type. Artifacts can't be combined (a data frame and model/source
        # bytes share no schema), so there's nothing to aggregate across requests.
        # Reject a second request at the source — both download() and retrieve()
        # funnel through here — so run() provably operates on a single dataflow.
        if self._requests:
            current_request = cast(
                "PFundComponentFeedRequest", self._get_current_request()
            )
            raise ValueError(
                f"{self.name} runs one artifact per feed: a "
                + f"{current_request.artifact_type} request is already queued. "
                + "Use a fresh feed (or run() the queued one) before requesting "
                + f"{request.artifact_type}."
            )
        return super()._append_request(request)

    def transform(self, *funcs: Callable[..., Any]) -> Self:
        raise NotImplementedError(
            f"{self.name} does not support transform(): artifacts are persisted as-is"
        )

    def _get_default_transformations_for_download(
        self, request: PFundComponentFeedDownloadRequest
    ) -> list[Callable[..., Any]]:
        from pfeed._etl.base import convert_dataframe
        from pfeed.utils import lambda_with_name

        default_transformations = []
        if request.artifact_type == ArtifactType.data:
            default_transformations.append(
                lambda_with_name(
                    "convert_to_user_df", lambda df: convert_dataframe(df)
                ),
            )
        return default_transformations

    def _get_default_transformations_for_retrieve(
        self, request: PFundComponentFeedRetrieveRequest
    ) -> list[Callable[..., Any]]:
        from pfeed._etl.base import convert_dataframe
        from pfeed.utils import lambda_with_name

        default_transformations = []
        if request.artifact_type == ArtifactType.data:
            default_transformations.append(
                lambda_with_name(
                    "convert_to_user_df", lambda df: convert_dataframe(df)
                ),
            )
        return default_transformations

    def download(
        self,
        artifact_type: ArtifactType | str = ArtifactType.data,
        env: Environment | str = Environment.BACKTEST,
        project_name: str = EngineContext.DEFAULT_PROJECT_NAME,
        run_id: str = EngineContext.DEFAULT_RUN_NAME,
        storage_config: StorageConfig | None = None,
    ) -> Self | RunResult:
        """Extract the component's artifact (e.g. .py source code, model weights, or output dataframe)c"""
        from pfeed.sources.pfund.requests import PFundComponentFeedDownloadRequest

        env = Environment[env.upper()]
        setup_logging(env=env)
        request = PFundComponentFeedDownloadRequest(
            artifact_type=artifact_type,
            env=env,
            project_name=project_name,
            run_id=run_id,
            storage_config=storage_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def _download_impl(
        self, data_model: PFundComponentDataModel
    ) -> pl.DataFrame | bytes:
        artifact_type = data_model.artifact_type
        if artifact_type == ArtifactType.source:
            return self.component_source_file.read_bytes()
        elif artifact_type == ArtifactType.model:
            return self.component.dump()
        elif artifact_type == ArtifactType.data:
            from pfeed._etl.base import convert_dataframe

            user_df = self.component.get_df(kind="output", to_native=True)
            # convert to polars for consistency with _download_impl return type in market feed
            return convert_dataframe(user_df, data_tool=DataTool.polars)
        else:
            raise ValueError(f"Unsupported artifact type: {artifact_type}")

    def _create_data_model_from_request(
        self, request: PFundComponentFeedRequest
    ) -> PFundComponentDataModel:
        return self.create_data_model(
            artifact_type=request.artifact_type, run_id=request.run_id
        )

    def create_data_model(
        self,
        artifact_type: ArtifactType | str = ArtifactType.data,
        env: Environment | str = Environment.BACKTEST,
        project_name: str = EngineContext.DEFAULT_PROJECT_NAME,
        run_id: str = EngineContext.DEFAULT_RUN_NAME,
    ) -> PFundComponentDataModel:
        artifact_type = ArtifactType[artifact_type.lower()]
        artifact_kwargs: dict[str, Any] = {
            "env": env,
            "project_name": project_name,
            "run_id": run_id,
            **self.component.to_dict(),
        }
        match artifact_type:
            case ArtifactType.model:
                from pfeed.sources.pfund.component_data_model import ModelArtifact
                from pfund.components.models.jax_model import JaxModel
                from pfund.components.models.pytorch_model import PytorchModel
                from pfund.components.models.sklearn_model import SklearnModel

                if isinstance(self.component, SklearnModel):
                    extension = ".joblib"
                elif isinstance(self.component, (PytorchModel, JaxModel)):
                    extension = ".safetensors"
                else:
                    raise ValueError(f"Unsupported model type: {type(self.component)}")
                return ModelArtifact(extension=extension, **artifact_kwargs)
            case ArtifactType.data:
                from pfeed.sources.pfund.component_data_model import DataArtifact

                return DataArtifact(**artifact_kwargs)
            case ArtifactType.source:
                from pfeed.sources.pfund.component_data_model import SourceArtifact

                return SourceArtifact(
                    filename=self.component_source_file.name, **artifact_kwargs
                )
            case _:
                assert_never(artifact_type)

    def retrieve(
        self,
        artifact_type: ArtifactType | str = ArtifactType.data,
        project_name: str = EngineContext.DEFAULT_PROJECT_NAME,
        run_id: str = EngineContext.DEFAULT_RUN_NAME,
        env: Environment | str = Environment.BACKTEST,
        storage_config: StorageConfig | None = None,
    ) -> Self | RunResult:
        """Read a component artifact back FROM STORAGE (read-back twin of download+load)."""
        from pfeed.sources.pfund.requests import PFundComponentFeedRetrieveRequest

        env = Environment[env.upper()]
        setup_logging(env=env)
        artifact_type = ArtifactType[artifact_type.lower()]
        storage_config = self._normalize_storage_config(
            storage_config or StorageConfig()
        )
        io_format = (
            IOFormat.DELTALAKE if artifact_type == ArtifactType.data else IOFormat.BLOB
        )
        io_config = self._normalize_io_config(IOConfig(io_format=io_format))
        request = PFundComponentFeedRetrieveRequest(
            env=env,
            project_name=project_name,
            run_id=run_id,
            data_source=self.name,
            artifact_type=artifact_type,
            storage_config_for_retrieval=storage_config,
            io_config_for_retrieval=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(data_model, request)
        )
        return self.run() if not self.is_pipeline() else self

    def _retrieve_impl(
        self,
        data_model: PFundComponentDataModel,
        request: PFundComponentFeedRetrieveRequest,
    ) -> Any:
        storage_config = request.storage_config_for_retrieval
        io_config = request.io_config_for_retrieval
        Storage = DataStorage[storage_config.storage].storage_class
        storage = Storage.from_storage_config(storage_config).with_io(io_config)
        _ = storage.with_data_model(data_model)
        artifact: pl.LazyFrame | bytes | None = storage.read()
        if artifact is not None:
            self.logger.debug(f"retrieved artifact {data_model} from {storage}")
        else:
            self.logger.debug(f"no artifact found for {data_model} in {storage}")
        return artifact

    # TODO: connect to mtflow's ws server
    def stream(self, *args: Any, **kwargs: Any) -> Self:
        # streaming a component's live signals — separate (StreamingFeedMixin) path
        raise NotImplementedError(f"{self.name} stream() is not implemented yet")

    def _create_batch_dataflows(
        self, extract_func: Callable[[PFundComponentDataModel], Any]
    ) -> list[DataFlow]:
        request = cast("PFundComponentFeedRequest", self._get_current_request())
        self.logger.debug(
            f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN
        )
        data_model = self._create_data_model_from_request(request)
        faucet = self._create_faucet(
            data_source=data_model.data_source,
            extract_func=extract_func,
            extract_type=request.extract_type,
        )
        dataflows = [self._create_dataflow(faucet=faucet, data_model=data_model)]
        self._dataflows[request] = dataflows
        return dataflows

    def run(self, **prefect_kwargs: Any) -> RunResult:
        from pfeed._etl.base import convert_dataframe
        from pfeed.dataflow.result import RunResult

        dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)
        # one component, one artifact type, one request (enforced in _append_request),
        # so there is exactly one dataflow — nothing to aggregate.
        [dataflow] = dataflows
        result: DataFlowResult = dataflow.result
        data: IntoFrame | bytes | None = result.data
        # NOTE: only data artifact returns dataframe
        is_dataframe = data is not None and not isinstance(data, bytes)
        if is_dataframe:
            data = convert_dataframe(data)
        return RunResult(data=data, dataflows=dataflows)
