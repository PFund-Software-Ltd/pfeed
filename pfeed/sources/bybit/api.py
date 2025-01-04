from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.exchanges.exchange_base import BaseExchange
    from pfund.products.product_base import BaseProduct


# TODO: add orderbook data support, one snapshot has 500 levels...can pandas handle this?
# url: e.g. https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2025-01-01_BTCUSDT_ob500.data.zip
# choices: https://api2.bybit.com/quote/public/support/download/list-options?bizType=contract&productId=orderbook
class BybitAPI:
    '''Custom API for downloading data from Bybit'''
    URLS = {
        'PERP': 'https://public.bybit.com/trading',
        'IPERP': 'https://public.bybit.com/trading',
        'FUT': 'https://public.bybit.com/trading',
        'IFUT': 'https://public.bybit.com/trading',
        'SPOT': 'https://public.bybit.com/spot',
    }
    DATA_NAMING_REGEX_PATTERNS = {
        'PERP': r'(USDT\/|PERP\/)$',  # USDT perp or USDC perp;
        'FUT': r'-\d{2}[A-Z]{3}\d{2}\/$',  # USDC futures e.g. BTC-10NOV23/
        'IPERP': r'USD\/$',  # inverse perps;
        'IFUT': r'USD[A-Z]\d{2}\/$',  # inverse futures e.g. BTCUSDH24/
        'SPOT': '.*',  # match everything since everything from https://public.bybit.com/spot is spot
    }

    def __init__(self, exchange: BaseExchange):
        self._exchange = exchange
        self.adapter = exchange.adapter
        # self._efilenames = {}
    
    @staticmethod
    def _get(url, frequency=1, num_retry=3):
        '''
        Handles general requests.get with control on frequency and number of retry
        '''
        import time
        import requests
        from requests.exceptions import ConnectionError
        
        from pfund import print_warning

        while num_retry:
            try:
                res = requests.get(url)
                if res.status_code == 200:
                    return res
                elif res.status_code == 404:
                    base_url = '/'.join(url.split('/')[:-1])
                    print_warning(f'File not found {url}, please go to {base_url} to check if the file exists')
                    break
                else:
                    print_warning(f'{res.status_code=} {res.text=}')
            except ConnectionError:
                print_warning(f'ConnectionError: failed to call {url}')
            except Exception as err:
                print_warning(f'Unhandled Error: failed to call {url}, {err=}')
            time.sleep(frequency)
        else:
            print_warning(f'failed to call {url}')

    @staticmethod
    def _create_efilename(epdt: str, date: str, is_spot: bool):
        if is_spot:
            return f'{epdt}_{date}.csv.gz'
        else:
            return f'{epdt}{date}.csv.gz'
        
    # def get_efilenames(self, ptype: str, epdt: str):
    #     '''Get external file names (e.g. BTCUSDT2022-10-04.csv.gz)'''
    #     from bs4 import BeautifulSoup
    #     url = '/'.join([self.URLS[ptype], epdt])
    #     if res := self._get(url, frequency=1, num_retry=3):
    #         soup = BeautifulSoup(res.text, 'html.parser')
    #         efilenames = [node.get('href') for node in soup.find_all('a')]
    #         return efilenames
    
    def get_epdts_by_ptype(self, ptype: str):
        '''Get external products based on product type'''
        import re
        from bs4 import BeautifulSoup
        ptype = ptype.upper()
        pattern = re.compile(self.DATA_NAMING_REGEX_PATTERNS[ptype])
        url = self.URLS[ptype]
        if res := self._get(url, frequency=1, num_retry=3):
            soup = BeautifulSoup(res.text, 'html.parser')
            epdts = [node.get('href').replace('/', '') for node in soup.find_all('a') if pattern.search(node.get('href'))]
            return epdts
    
    def get_data(self, product: BaseProduct, date: str) -> bytes | None:
        # used to check if the efilename created by the date exists in the efilenames (files on the exchange's data server)
        if product.is_option():
            raise NotImplementedError('Bybit does not provide options data')
        epdt, ptype = product.symbol, product.type.value
        efilename = self._create_efilename(epdt, date, product.is_spot())
        url = f"{self.URLS[ptype]}/{epdt}/{efilename}"
        if res := self._get(url, frequency=1, num_retry=3):
            data = res.content
            return data
