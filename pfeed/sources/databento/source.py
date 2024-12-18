from __future__ import annotations
from typing import TYPE_CHECKING, Any, Literal
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

import os
import datetime

import databento
# from databento_dbn import SType
from databento.common.publishers import Dataset, Publisher
from databento.common.dbnstore import DBNStore

from pfeed.sources.tradfi_source import TradFiSource
from pfeed.sources.databento.const import DATASETS


tDATASET = Literal[
    'DBEQ.BASIC', 
    'GLBX.MDP3', 
    'NDEX.IMPACT', 
    'IFEU.IMPACT', 
    'OPRA.PILLAR', 
    'XNAS.ITCH'
]


# NOTE: 2024-10-26, best free llm for deriving databento dataset and start date is 'mistral'
class DatabentoSource(TradFiSource):
    def __init__(self):
        super().__init__('DATABENTO')
        if not os.getenv('DATABENTO_API_KEY'):
            raise ValueError('"DATABENTO_API_KEY" is not set in .env file')
        api_key = os.getenv('DATABENTO_API_KEY')
        self.hist_api = self.historical_api = databento.Historical(api_key)
        self.live_api = databento.Live(api_key)
        self.ref_api = self.reference_api = databento.Reference(api_key)

    # TODO
    def create_product(self, product_basis: str, symbol='', **product_specs) -> BaseProduct:
        product = super().create_product(product_basis, symbol, **product_specs)
        if not symbol:
            symbol = self._create_symbol(product)
            product.set_symbol(symbol)
        return product
    
    # TODO
    def _create_symbol(self, product: BaseProduct) -> str:
        pass
    
    def _derive_start_date(self, symbol: str) -> str | None:
        def is_valid_date(date_string):
            try:
                datetime.datetime.strptime(date_string, "%Y-%m-%d")
                return True
            except ValueError:
                return False
        try:
            start_date = self.derive_start_date_using_llm(symbol)
            if is_valid_date(start_date):
                return start_date
        except:
            return None

    @require_plugin(LlmPlugin)
    def derive_start_date_using_llm(self, symbol: str) -> str | None:
        '''Derive the start date for a symbol using LLM.'''
        # start=(datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d'),
        from pfeed import llm
        from pfund.const.common import FUTURES_MONTH_CODES
        context = f'''
        You are a financial data expert. 
        Given a symbol and the current year, you are asked to return a date string in the format YYYY-MM-DD
        representing a date when the symbol is or was actively traded.
        FUTURES_MONTH_CODES is provided for your reference if the symbol is a futures contract:
        {FUTURES_MONTH_CODES}
        only return the date string, nothing else, no explanation.
        '''
        return llm.ask(f'{symbol=} current_year={datetime.datetime.now().year}', context=context).strip()
    
    def _derive_end_date(self, start_date: str) -> str:
        '''Derive the end date to be the day after the start date.'''
        return (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    def _derive_dataset(self, symbol: str) -> tDATASET | None:
        try:
            dataset = self.derive_dataset_using_llm(symbol)
            if dataset in DATASETS:
                return dataset
        except:
            return None
    
    @require_plugin(LlmPlugin)
    def derive_dataset_using_llm(self, symbol: str) -> tDATASET | None:
        '''Derive the dataset for a symbol using LLM.
        e.g. symbol=AAPL, use LLM to get the corresponding dataset
        '''
        from pfeed import llm
        context = f'''
        You are a financial data expert. 
        Given a symbol, you are asked to determine the corresponding dataset.
        {DATASETS}
        only return the dataset name, which must be one of the dictionary keys above, nothing else, no explanation.
        '''
        return llm.ask(symbol, context=context).strip().replace('"', '').replace("'", '')

    # TODO: extend databento's Dataset enum by adding product types to it?    
    def get_datasets(self):
        return DATASETS
    
    def get_dataset_description(self, dataset: tDATASET) -> str:
        return Dataset(dataset).description
    
    def list_publishers(self):
        return self.hist_api.metadata.list_publishers()
    
    def list_datasets(self):
        return self.hist_api.metadata.list_datasets()
    
    def convert_instrument_id_to_raw_symbol(self, 
        dataset: Dataset | str, 
        instrument_id: str | int,
        start_date: str,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        res = self.hist_api.symbology.resolve(
            dataset,
            symbols=instrument_id,
            stype_in='instrument_id', 
            stype_out='raw_symbol', 
            start_date=start_date, 
            end_date=end_date
        )
        return res['result'][str(instrument_id)][0]['s']
        
    def get_child_instruments(self, parent_symbol: str, dataset: Dataset | str | None = None, start_date: str | None = None, end_date: str | None = None) -> DBNStore:
        dataset = dataset or self._derive_dataset(parent_symbol)
        if not dataset:
            raise ValueError('Failed to derive dataset, please specify "dataset" or call pe.add_plugin(LlmPlugin(...))() to set up for LLM usage if you haven not done so')
        start_date = start_date or self._derive_start_date(parent_symbol)
        if not start_date:
            raise ValueError('Failed to derive start date, please specify "start_date" or call pe.add_plugin(LlmPlugin(...))() to set up for LLM usage if you haven not done so')
        data = self.hist_api.timeseries.get_range(
            dataset=dataset,
            stype_in="parent",
            symbols=parent_symbol,
            schema="definition",
            start=start_date,
            end=end_date or self._derive_end_date(start_date),
        )
        # return data.to_df()[["instrument_id", "raw_symbol"]]
        return data

    
    # TODO: not supported yet, use it to extend get_symbol_definition()?
    def get_security_master(self):
        return self.ref_api.security_master.get_range(...)
    
    def get_symbol_definition(self, symbol: str, dataset: Dataset | str | None = None, start_date: str | None = None, end_date: str | None = None) -> dict:
        '''Get a brief definition of a symbol.'''
        dataset = dataset or self._derive_dataset(symbol)
        if not dataset:
            raise ValueError('Failed to derive dataset, please specify "dataset" or call pe.add_plugin(LlmPlugin(...))() to set up for LLM usage if you haven not done so')
        start_date = start_date or self._derive_start_date(symbol)
        if not start_date:
            raise ValueError('Failed to derive start date, please specify "start_date" or call pe.add_plugin(LlmPlugin(...))() to set up for LLM usage if you haven not done so')
        raw_definition: DBNStore = self.hist_api.timeseries.get_range(
            dataset=dataset,
            schema='definition',
            symbols=symbol,
            start=start_date,
            end=end_date or self._derive_end_date(start_date),
        )
        raw_df = raw_definition.to_df().iloc[0]
        publisher = Publisher.from_int(raw_df['publisher_id'])
        definition = {
            'publisher': publisher.description,
            'venue': publisher.venue.description,
            'dataset': publisher.dataset.description,
            'security_type': raw_df['security_type'],
            'instrument_class': raw_df['instrument_class'],
            'raw_symbol': raw_df['raw_symbol'],
            'symbol': raw_df['symbol'],
            'exchange': raw_df['exchange'],
            'asset': raw_df['asset'],
            'currency': raw_df['currency'],
            'underlying': raw_df['underlying'],
            'strike_price': raw_df['strike_price'],
            'strike_price_currency': raw_df['strike_price_currency'],
            'expiration': raw_df['expiration'],
            'raw_definition': raw_definition
        }
        return definition