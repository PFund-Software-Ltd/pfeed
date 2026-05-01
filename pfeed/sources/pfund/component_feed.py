from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Any, Callable
if TYPE_CHECKING:
    from pfund.enums import ComponentType
    from pfund.typing import Component
    from pfeed.sources.pfund.component_data_model import BaseArtifact, ModelArtifact, DataArtifact, SourceArtifact

from pathlib import Path

from pfund.enums import Environment, ArtifactType
from pfeed.sources.pfund.mixin import PFundMixin
from pfeed.feeds.base_feed import BaseFeed
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.enums import IOFormat, DataLayer


# REVIEW: currently skip BaseFeed's request->dataflow structure.
# e.g. in load(), there is no dataflow or request created in ComponentFeed.
# if it were to be implemented, add back num_workers parameter to __init__()
class ComponentFeed(PFundMixin, BaseFeed):  # pyright: ignore[reportImplicitAbstractClass]
    def __init__(self):
        # NOTE: no pipeline mode support for now
        super().__init__(pipeline_mode=False)
        self._component: Component | None = None
        
    def __call__(self, component: Component) -> ComponentFeed:
        self._component = component
        return self
    
    @property
    def component(self) -> Component:
        assert self._component is not None, 'component is not set'
        return self._component
    
    @property
    def component_type(self) -> ComponentType:
        return self.component.component_type
    
    def transform(self, *funcs: Callable[..., Any]) -> ComponentFeed:
        raise NotImplementedError(f'{self.name} transform() is not implemented')
        return self
    
    def _create_data_artifact(self) -> DataArtifact:
        from pfeed.sources.pfund.component_data_model import DataArtifact
        component_metadata = self.component.to_dict()
        return DataArtifact(
            artifact_type=ArtifactType.data,
            component_type=self.component_type,
            class_name=component_metadata['class_name'],
            extension='.delta',
        )
    
    def _create_source_artifact(self) -> SourceArtifact:
        from pfeed.sources.pfund.component_data_model import SourceArtifact
        component_metadata = self.component.to_dict()
        return SourceArtifact(
            artifact_type=ArtifactType.source,
            component_type=self.component_type,
            class_name=component_metadata['class_name'],
            extension='.py',
        )
    
    def _create_model_artifact(self) -> ModelArtifact:
        from pfeed.sources.pfund.component_data_model import ModelArtifact
        from pfund.components.models.sklearn_model import SklearnModel
        from pfund.components.models.pytorch_model import PytorchModel
        assert self.component.is_model(), f'{self.component} is not a model'
        if isinstance(self.component, SklearnModel):
            extension = '.joblib'
        elif isinstance(self.component, PytorchModel):
            extension = '.pth'
        else:
            raise ValueError(f'{self.component} is not a supported model')
        component_metadata = self.component.to_dict()
        return ModelArtifact(
            artifact_type=artifact_type,
            component_type=self.component_type,
            # TODO:
            # size_bytes=...  
            class_name=component_metadata['class_name'],
            extension=extension,
        )
    
    def create_data_model(self, artifact_type: ArtifactType | str) -> BaseArtifact:
        artifact_type = ArtifactType[artifact_type.upper()]
        if artifact_type == ArtifactType.model:
            artifact = self._create_model_artifact()
        elif artifact_type == ArtifactType.data:
            artifact = self._create_data_artifact()
        elif artifact_type == ArtifactType.source:
            artifact = self._create_source_artifact()
        else:
            raise ValueError(f'{artifact_type} is not supported')
        return artifact
    
    # TODO: download component's artifacts that are stored in cloud?
    # def _create_data_model_from_request(self, request):
    #     pass
    # def download(self, *args, **kwargs):
    #     raise NotImplementedError(f'{self.name} download() is not implemented')
    # def _download_impl(self, data_model: ComponentDataModel):
    #     pass
    # def _get_default_transformations_for_download(self, *args, **kwargs):
    #     pass

    def retrieve(self, label: str=''):
        '''
        Retrieve component artifacts from storage.
        Args:
            label: Unique label to identify the component.
                It MUST be provided when env is PAPER/LIVE to make sure the correct component is retrieved.
        '''
        pass

    def _retrieve_impl(self, data_model: BaseArtifact):
        pass

    def _get_default_transformations_for_retrieve(self, *args, **kwargs):
        pass
    
    # TODO: stream component's signals
    def stream(
        self, 
        component_name: str, 
        component_type: str | ComponentType, 
        engine_name: str='engine', 
        env: Environment | str=Environment.BACKTEST,
    ):
        pass
    
    def _stream_impl(self, data_model: BaseArtifact):
        pass
    
    def _create_batch_dataflows(self, *args, **kwargs):
        pass

    def load(
        self,
        *,
        artifact_type: ArtifactType = ArtifactType.data,
        storage: FileBasedDataStorage = FileBasedDataStorage.LOCAL,
        data_path: Path | str | None = None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = '',
        storage_options: dict[str, Any] | None = None,
        **io_kwargs: Any,
    ) -> ComponentFeed:
        from pfeed.storages.storage_config import StorageConfig
        storage_config = StorageConfig(
            storage=storage, 
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            io_format=IOFormat.DELTALAKE,  # NOTE: only used to write data artifacts
            storage_options=storage_options,
        )
        data_model = self.create_data_model(artifact_type)
        Storage = storage_config.storage.storage_class
        storage = (
            Storage
            .from_storage_config(storage_config)
            .with_data_model(data_model)
            .with_io(io_config)
        )
        fs = storage.get_filesystem()
        engine_name = self.component.context.name
        artifact_path = (
            storage_config.data_path /
            engine_name /
            self.component_type.to_plural() /
            self.component.name
        )
        
        if artifact_type == ArtifactType.data:
            pass
        elif artifact_type == ArtifactType.model:
            if self.component_type != ComponentType.model:
                raise ValueError(f'{self.component} must be a model when loading {artifact_type=})')
            try:
                from pfund.components.models.sklearn_model import SklearnModel
            except ImportError:
                SklearnModel = None
            try:
                from pfund.components.models.pytorch_model import PytorchModel
            except ImportError:
                PytorchModel = None
            
            if SklearnModel and isinstance(self.component, SklearnModel):
                import json
                import joblib

                # write sklearn model using joblib
                file_path = artifact_path / f"{self.component.name}.joblib"
                with fs.open_output_stream(file_path) as f:
                    joblib.dump(self.component.model, f, compress=3)
                
                # TEMP
                self.logger.warning(f'Dumped sklearn model {self.component.name} to {file_path}')
                
                # write metadata
                # metadata = self.component.to_dict()
                # with fs.open_output_stream(artifact_path / "metadata.json") as f:
                #     f.write(json.dumps(metadata, indent=2).encode())

            elif PytorchModel and isinstance(self.component, PytorchModel):
                import torch
                torch.save(self.component.state_dict(), f"{self.component.name}.pth")
            else:
                raise ValueError(f'{self.name} load() is not supported for model: {self.component}')
        # TODO: write .py files
        elif artifact_type == ArtifactType.source:
            pass
        else:
            raise ValueError(f'{self.name} load() is not supported for {self.component} ({artifact_type=})')
        return self

    
    def run(self, **prefect_kwargs):
        pass
