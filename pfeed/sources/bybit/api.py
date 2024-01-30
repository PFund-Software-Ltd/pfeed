import re
import time
import logging

import requests

from bs4 import BeautifulSoup
from pfeed.sources.bybit.const import DATA_SOURCE, DATA_SOURCE_URLS, DATA_NAMING_REGEX_PATTERNS, create_efilename


logger = logging.getLogger(DATA_SOURCE.lower())


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


def get_efilenames(category: str, epdt: str):
    '''
    Get efilenames (e.g. BTCUSDT2022-10-04.csv.gz)
    '''
    def _handle_response(res):
        soup = BeautifulSoup(res.text, 'html.parser')
        efilenames = [node.get('href') for node in soup.find_all('a')]
        return efilenames
    url = '/'.join([DATA_SOURCE_URLS[category], epdt])
    return get(url, _handle_response, frequency=1, num_retry=3)
    

def get_epdts(category: str, ptype: str):
    def _handle_response(res):
        soup = BeautifulSoup(res.text, 'html.parser')
        epdts = [node.get('href').replace('/', '') for node in soup.find_all('a') if pattern.search(node.get('href'))]
        return epdts
    pattern = re.compile(DATA_NAMING_REGEX_PATTERNS[ptype])
    url = DATA_SOURCE_URLS[category]
    return get(url, _handle_response, frequency=1, num_retry=3)


def get_data(category: str, epdt: str, date: str):
    def _handle_response(res):
        data = res.content
        return data
    efilename = create_efilename(epdt, date, is_spot=(category.upper()=='SPOT'))
    url = f"{DATA_SOURCE_URLS[category]}/{epdt}/{efilename}"
    return get(url, _handle_response, frequency=1, num_retry=3)