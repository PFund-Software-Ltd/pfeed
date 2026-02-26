from pfeed.sources.pfund.source import PFundSource


class PFundMixin:
    data_source: PFundSource

    @staticmethod
    def _create_data_source() -> PFundSource:
        return PFundSource()