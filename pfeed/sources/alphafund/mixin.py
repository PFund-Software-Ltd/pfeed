from pfeed.sources.alphafund.source import AlphaFundSource


class AlphaFundMixin:
    data_source: AlphaFundSource

    @staticmethod
    def _create_data_source() -> AlphaFundSource:
        return AlphaFundSource()
