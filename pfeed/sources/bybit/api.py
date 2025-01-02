from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct


class BybitAPI:
    '''Custom API for downloading data from Bybit'''
    URLS = {
        'PERP': 'https://public.bybit.com/trading',
        'IPERP': 'https://public.bybit.com/trading',
        'FUT': 'https://public.bybit.com/trading',
        'IFUT': 'https://public.bybit.com/trading',
        'OPT': 'https://public.bybit.com/trading',
        'SPOT': 'https://public.bybit.com/spot',
    }
    DATA_NAMING_REGEX_PATTERNS = {
        'PERP': r'(USDT\/|PERP\/)$',  # USDT perp or USDC perp;
        'FUT': r'-\d{2}[A-Z]{3}\d{2}\/$',  # USDC futures e.g. BTC-10NOV23/
        'IPERP': r'USD\/$',  # inverse perps;
        'IFUT': r'USD[A-Z]\d{2}\/$',  # inverse futures e.g. BTCUSDH24/
        'SPOT': '.*',  # match everything since everything from https://public.bybit.com/spot is spot
        # TODO: add options
        # 'OPT': ...
    }

    def __init__(self, exchange):
        self._exchange = exchange
        self.adapter = exchange.adapter
        # self.efilenames = {}
    
    @staticmethod
    def _get(url, frequency=1, num_retry=3):
        '''
        Handles general requests.get with control on frequency and number of retry
        '''
        import time
        import requests
        from requests.exceptions import ConnectionError
        
        from pfund import print_error, print_warning

        while num_retry:
            try:
                res = requests.get(url)
                if res.status_code == 200:
                    return res
                elif res.status_code == 404:
                    print_error(f'File not found {url=} {res.status_code=} {res.text=}')
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

    def get_efilenames(self, pdt: str):
        '''
        Get external file names (e.g. BTCUSDT2022-10-04.csv.gz)
        '''
        from bs4 import BeautifulSoup
        ptype = pdt.split('_')[2].upper()
        category = self._exchange._derive_product_category(ptype)
        epdt = self.adapter(pdt, group=category)
        url = '/'.join([self.URLS[ptype], epdt])
        if res := self._get(url, frequency=1, num_retry=3):
            soup = BeautifulSoup(res.text, 'html.parser')
            efilenames = [node.get('href') for node in soup.find_all('a')]
            return efilenames
        
    def get_epdts(self, ptype: str):
        '''Get external products'''
        import re
        from bs4 import BeautifulSoup
        pattern = re.compile(self.DATA_NAMING_REGEX_PATTERNS[ptype])
        url = self.URLS[ptype]
        if res := self._get(url, frequency=1, num_retry=3):
            soup = BeautifulSoup(res.text, 'html.parser')
            epdts = [node.get('href').replace('/', '') for node in soup.find_all('a') if pattern.search(node.get('href'))]
            return epdts

    def get_data(self, product: BaseProduct, date: str) -> bytes | None:
        def _create_efilename(ptype: str, epdt: str, date: str):
            is_spot = (ptype == 'SPOT')
            if is_spot:
                return f'{epdt}_{date}.csv.gz'
            else:
                return f'{epdt}{date}.csv.gz'
        # used to check if the efilename created by the date exists in the efilenames (files on the exchange's data server)
        # if pdt not in self.efilenames:
        #     self.efilenames[pdt] = self.get_efilenames(pdt)
        ptype = product.type.value
        epdt = product.symbol
        efilename = _create_efilename(ptype, epdt, date)
        # if efilename not in self.efilenames[pdt]:
        #     return None
        url = f"{self.URLS[ptype]}/{epdt}/{efilename}"
        if res := self._get(url, frequency=1, num_retry=3):
            data = res.content
            return data
