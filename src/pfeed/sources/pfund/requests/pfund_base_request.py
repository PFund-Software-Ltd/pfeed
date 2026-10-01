from pydantic import field_validator

from pfeed.enums import DataSource
from pfeed.requests.base_request import BaseRequest
from pfund.enums import Environment


class PFundBaseRequest(BaseRequest):
    data_source: DataSource | str = DataSource.PFUND

    env: Environment | str
    project_name: str
    run_name: str

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: Environment | str) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("project_name", mode="before")
    @classmethod
    def _lower_project_name(cls, value: str) -> str:
        return value.lower()

    @field_validator("run_name", mode="before")
    @classmethod
    def _lower_run_name(cls, value: str) -> str:
        return value.lower()
