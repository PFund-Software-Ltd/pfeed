from pfeed.enums import ExtractType
from pfeed.sources.alphafund.requests.fund_base_request import AlphaFundFeedBaseRequest


class AlphaFundFeedDownloadRequest(AlphaFundFeedBaseRequest):
    extract_type: ExtractType = ExtractType.download
