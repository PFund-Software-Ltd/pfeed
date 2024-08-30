import re
import time
import logging

import requests

from pfund.exchanges.binance.exchange import Exchange
from pfeed.sources.binance.const import DATA_SOURCE, DATA_SOURCE_URLS, DATA_NAMING_REGEX_PATTERNS, PTYPE_TO_CATEGORY, create_efilename


logger = logging.getLogger(DATA_SOURCE.lower() + '_data')


# EXTEND: support more browsers
# TODO: finish it
def browser_get(url, handle_func, frequency=1, num_retry=3, wait_x_secs_for_js=3):
    '''
    Handles general browser get
    Args:
        handle_func: specific logic for handling response
        wait_x_secs_for_js: wait for JavaScript to load content
    '''
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from webdriver_manager.chrome import ChromeDriverManager

    service = Service(ChromeDriverManager().install())

    logger.debug(f'calling {url} from browser')
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    
    # Wait for JavaScript to load content
    time.sleep(wait_x_secs_for_js)
    
    return handle_func(driver)


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

#TODO: 
def get_efilenames(pdt: str):
    '''
    Get efilenames
        Examples of the market data file's name of different products types in Binance Vision:
            PERP: BTCUSDT-trades-2024-04-09.zip
            FUT: BTCUSDT_240927-trades-2024-04-06.zip
            IPERP: BTCUSD_PERP-trades-2024-04-08.zip
            IFUT: BTCUSD_240927-trades-2024-04-09.zip
            SPOT: BTCUSDT-trades-2024-04-06.zip  
    '''
    def _handle_response(res):
        soup = BeautifulSoup(res.text, 'html.parser')
        efilenames = [node.get('href') for node in soup.find_all('a')]
        return efilenames
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    exchange = Exchange(env='LIVE', ptype=ptype)
    adapter = exchange.adapter
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    url = '/'.join([DATA_SOURCE_URLS[ptype], epdt]) #FIXME: IS this correct?
    return get(url, _handle_response, frequency=1, num_retry=3)
    

def get_epdts(ptype: str):
    def _handle_response(driver):
        res = driver.find_elements(By.TAG_NAME, 'tr')
        pattern = re.compile(DATA_NAMING_REGEX_PATTERNS[ptype])
        epdts = [r.text.replace('/', '') for r in res if pattern.search(r.text)]
        driver.quit()
        return epdts 
    url = DATA_SOURCE_URLS[ptype]
    url = url.replace('/data/', '/?prefix=data/')
    return browser_get(url, _handle_response, frequency=1, num_retry=3)


def get_data(pdt: str, date: str):
    def _handle_response(res):
        data = res.content
        return data
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    exchange = Exchange(env='LIVE', ptype=ptype)
    adapter = exchange.adapter
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    efilename = create_efilename(pdt, date)
    url = f"{DATA_SOURCE_URLS[ptype]}/{epdt}/{efilename}"
    return get(url, _handle_response, frequency=1, num_retry=3)