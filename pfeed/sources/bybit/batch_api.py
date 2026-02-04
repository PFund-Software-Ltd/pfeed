from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from httpx import Response
    from pfund.products.product_bybit import BybitProduct
    from pfund.datas.resolution import Resolution
    from pfund.typing import tEnvironment
    
import datetime

from pfund.enums import Environment, CryptoAssetType, AssetTypeModifier


# NOTE: do NOT trust the catalog in https://public.bybit.com/trading/, BTC_USDT_FUTs are missing, 
# e.g. BTCUSDT-22AUG25 is missing in the catalog but its url is valid: https://public.bybit.com/trading/BTCUSDT-22AUG25/
# the same applies to https://quote-saver.bycsi.com/orderbook/, 'spot' is missing in the catalog but its url is valid: https://quote-saver.bycsi.com/orderbook/spot
'''
Bybit's naming conventions for different contracts (non-spot, no options data):
BTCUSDT = BTC_USDT_PERP
BTCUSDT-22AUG25 = BTC_USDT_FUT
BTCPERP = BTC_USDC_PERP
BTC-30AUG24 = BTC_USDC_FUT
BTCUSD = BTC_USD_IPERP
BTCUSDH25 = BTC_USD_IFUT
'''
# url: e.g. https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2025-01-01_BTCUSDT_ob500.data.zip
# NOTE: productId=orderbook/trade, bizType=contract/spot
# See downloadable symbols: 
# https://www.bybit.com/x-api/quote/public/support/download/list-options?bizType={contract_or_spot}&productId={orderbook_or_trade}
class BatchAPI:
    '''Custom API for downloading data from Bybit'''
    DATA_NAMING_REGEX_PATTERNS = {
        CryptoAssetType.PERPETUAL: r'(USDT\/|PERP\/)$',  # USDT perp or USDC perp;
        CryptoAssetType.FUTURE: r'-\d{2}[A-Z]{3}\d{2}\/$',  # USDC futures e.g. BTC-10NOV23/
        AssetTypeModifier.INVERSE + '-' + CryptoAssetType.PERPETUAL: r'USD\/$',  # inverse perps;
        AssetTypeModifier.INVERSE + '-' + CryptoAssetType.FUTURE: r'USD[A-Z]\d{2}\/$',  # inverse futures e.g. BTCUSDH24/
        CryptoAssetType.CRYPTO: '.*',  # match everything since everything from https://public.bybit.com/spot is spot
    }

    def __init__(self, env: tEnvironment='BACKTEST'):
        env = Environment[env.upper()]
        if env != Environment.BACKTEST:
            from pfund.exchanges.bybit.rest_api import RESTfulAPI
            # TODO: use rest api to support fetch()?
            self._rest_api = RESTfulAPI(env)
        else:
            self._rest_api = None
    
    @property
    def env(self) -> Environment:
        if self._rest_api is None:
            return Environment.BACKTEST
        return self._rest_api._env
        
    @staticmethod
    def _get_base_url(product: BybitProduct, resolution: Resolution) -> str:
        if resolution.is_quote():
            if product.is_spot():
                return 'https://quote-saver.bycsi.com/orderbook/spot'
            elif product.is_inverse():
                return 'https://quote-saver.bycsi.com/orderbook/inverse'
            else:
                return 'https://quote-saver.bycsi.com/orderbook/linear'
        else:
            if product.is_spot():
                return 'https://public.bybit.com/spot'
            else:
                return 'https://public.bybit.com/trading'
    
    @staticmethod
    def _create_filename(product: BybitProduct, resolution: Resolution, date: str):
        from pfund_kit.utils.temporal import convert_to_date
        if resolution.is_quote():
            orderbook_levels = 200
            # NOTE: somehow after this date, the orderbook (non-spot) levels are changed from 500 to 200
            cutoff_date = '2025-08-21'
            is_before_cutoff = convert_to_date(date) < convert_to_date(cutoff_date)
            if not product.is_spot():
                if is_before_cutoff:
                    orderbook_levels = 500
            return f'{date}_{product.symbol}_ob{orderbook_levels}.data.zip'
        else:
            if product.is_spot():
                return f'{product.symbol}_{date}.csv.gz'
            else:
                return f'{product.symbol}{date}.csv.gz'
    
    @staticmethod
    def _get(url: str, params: dict | None=None):
        import time
        import httpx
        from pfund_kit.style import cprint, TextStyle, RichColor
        
        NUM_RETRY = 3
        while NUM_RETRY:
            NUM_RETRY -= 1
            try:
                response: Response = httpx.get(url, params=params)
                result = response.raise_for_status().content
                return result
            except httpx.RequestError as exc:
                cprint(f'RequestError: failed to get data from {url}, {exc=}', style=TextStyle.BOLD + RichColor.RED)
            except Exception as exc:
                cprint(f'Error: failed to get data from {url}, {exc=}', style=TextStyle.BOLD + RichColor.RED)
            time.sleep(1)
        else:
            cprint(f'Failed to get data from {url}', style=TextStyle.BOLD + RichColor.RED)
            return None
    
    def _create_url(self, product: BybitProduct, resolution: Resolution, date: str) -> str:
        base_url = self._get_base_url(product, resolution)
        filename = self._create_filename(product, resolution, date)
        url = f'{base_url}/{product.symbol}/{filename}'
        return url
    
    def _convert_orderbook_data_to_df(self, zipped_data: bytes) -> pd.DataFrame:
        import pandas as pd
        from msgspec import json
        from pfeed.enums.compression import Compression
        decoder = json.Decoder()
        data = Compression.decompress(zipped_data)
        data_str = data.decode("utf-8").strip().split('\n')
        df = pd.json_normalize([decoder.decode(item) for item in data_str])
        return df
    
    def get_data(self, product: BybitProduct, resolution: Resolution, date: str) -> bytes | pd.DataFrame | None:
        if product.is_option():
            raise NotImplementedError('Bybit does not provide options data')
        # TODO: it's quote_L2 data, need to support converting to quote_L1, converting to different number of levels (e.g. 10quote_L1) etc.
        # NOTE for the 'data.u' field in data:
        # Update ID. Is a sequence. Occasionally, you'll receive "u"=1, which is a snapshot data due to the restart of the service. 
        # So please overwrite your local orderbook
        if resolution.is_quote():
            raise NotImplementedError('orderbook data is not supported yet')
        url = self._create_url(product, resolution, date)
        if data := self._get(url):
            if resolution.is_quote():
                data: pd.DataFrame = self._convert_orderbook_data_to_df(data)
            return data

    # def _get_downloadable_symbols(self, ptype: str, epdt: str):
    #     '''Get external file names (e.g. BTCUSDT2022-10-04.csv.gz)'''
    #     from bs4 import BeautifulSoup
    #     url = '/'.join([self.URLS[ptype], epdt])
    #     if res := self._get(url, frequency=1, num_retry=3):
    #         soup = BeautifulSoup(res.text, 'html.parser')
    #         efilenames = [node.get('href') for node in soup.find_all('a')]
    #         return efilenames
    
    # def _get_symbols_by_asset_type(self, ptype: str):
    #     import re
    #     from bs4 import BeautifulSoup
    #     ptype = ptype.upper()
    #     pattern = re.compile(self.DATA_NAMING_REGEX_PATTERNS[ptype])
    #     url = self.URLS[ptype]
    #     if res := self._get(url):
    #         soup = BeautifulSoup(res.text, 'html.parser')
    #         epdts = [node.get('href').replace('/', '') for node in soup.find_all('a') if pattern.search(node.get('href'))]
    #         return epdts
