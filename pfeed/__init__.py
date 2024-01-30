import multiprocessing

from dotenv import load_dotenv, find_dotenv
from rich.console import Console

from pfeed.sources import bybit
from pfeed.feeds import YahooFinanceFeed, BybitFeed


cprint = Console().print
load_dotenv(find_dotenv())
multiprocessing.set_start_method('fork', force=True)


__all__ = (
    'bybit',
    'YahooFinanceFeed',
    'BybitFeed',
    'cprint',
)