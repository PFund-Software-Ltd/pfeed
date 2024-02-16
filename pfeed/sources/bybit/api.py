import re
import time
import logging

import requests

from bs4 import BeautifulSoup
from pfeed.sources.bybit.const import DATA_SOURCE, DATA_SOURCE_URLS, DATA_NAMING_REGEX_PATTERNS, PTYPE_TO_CATEGORY, create_efilename
from pfund.exchanges.bybit.exchange import Exchange


logger = logging.getLogger(DATA_SOURCE.lower())
exchange = Exchange(env='LIVE')
adapter = exchange.adapter


def get(url, handle_func, frequency=1, num_retry=3):
    '''
    Handles general requests.get with control on frequency and number of retry
    Args:
        handle_func: specific logic for handling response
    '''
    logger.debug(f'calling {url}')
    while num_retry:
        res = requests.get(url)
        if res.status_code == 200:
            return handle_func(res)
        elif res.status_code == 404:
            logger.error(f'File not found {url=} {res.status_code=} {res.text=}')
            break
        else:
            logger.warning(f'{res.status_code=} {res.text=}')
        time.sleep(frequency)
    else:
        logger.error(f'failed to call {url}')


def get_efilenames(pdt: str):
    '''
    Get efilenames (e.g. BTCUSDT2022-10-04.csv.gz)
    '''
    def _handle_response(res):
        soup = BeautifulSoup(res.text, 'html.parser')
        efilenames = [node.get('href') for node in soup.find_all('a')]
        return efilenames
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    url = '/'.join([DATA_SOURCE_URLS[ptype], epdt])
    return get(url, _handle_response, frequency=1, num_retry=3)
    

def get_epdts(ptype: str):
    def _handle_response(res):
        soup = BeautifulSoup(res.text, 'html.parser')
        epdts = [node.get('href').replace('/', '') for node in soup.find_all('a') if pattern.search(node.get('href'))]
        return epdts
    pattern = re.compile(DATA_NAMING_REGEX_PATTERNS[ptype])
    url = DATA_SOURCE_URLS[ptype]
    return get(url, _handle_response, frequency=1, num_retry=3)


def get_data(pdt: str, date: str):
    def _handle_response(res):
        data = res.content
        return data
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    efilename = create_efilename(pdt, date)
    url = f"{DATA_SOURCE_URLS[ptype]}/{epdt}/{efilename}"
    return get(url, _handle_response, frequency=1, num_retry=3)