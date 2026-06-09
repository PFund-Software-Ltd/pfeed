from pydantic import field_validator

from pfeed.sources.pfund.requests.pfund_base_request import PFundBaseRequest
from pfund.enums import ArtifactType


class PFundComponentFeedBaseRequest(PFundBaseRequest):
    artifact_type: ArtifactType | str

    @field_validator("artifact_type", mode="before")
    @classmethod
    def _validate_artifact_type(cls, value: ArtifactType | str) -> ArtifactType:
        if isinstance(value, str):
            return ArtifactType[value.lower()]
        return value
