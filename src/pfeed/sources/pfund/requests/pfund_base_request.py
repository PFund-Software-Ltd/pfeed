from pydantic import field_validator

from pfeed.enums import DataSource
from pfeed.requests.base_request import BaseRequest
from pfund.enums import Environment, RunStage


class PFundBaseRequest(BaseRequest):
    data_source: DataSource | str = DataSource.PFUND

    env: Environment | str
    run_stage: RunStage | str
    project_name: str
    run_id: str

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: Environment | str) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("run_stage", mode="before")
    @classmethod
    def _validate_run_stage(cls, value: RunStage | str) -> RunStage:
        if isinstance(value, str):
            return RunStage[value]
        return value

    @field_validator("project_name", mode="before")
    @classmethod
    def _lower_project_name(cls, value: str) -> str:
        return value.lower()

    @field_validator("run_id", mode="before")
    @classmethod
    def _lower_run_id(cls, value: str) -> str:
        return value.lower()
