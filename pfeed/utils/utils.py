from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import (
        tSUPPORTED_DOWNLOAD_DATA_SOURCES, 
        tSUPPORTED_STORAGES,
    )

import re
import datetime
import calendar
import pytz

from pfeed.const.common import SUPPORTED_CRYPTO_EXCHANGES, SUPPORTED_STORAGES


def derive_trading_venue(data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES) -> str:
    if data_source.upper() in SUPPORTED_CRYPTO_EXCHANGES:
        trading_venue = data_source.upper()
    else:
        trading_venue = 'SMART'
    return trading_venue


def get_available_data_storages() -> list[tSUPPORTED_STORAGES]:
    from pfeed.datastore import check_if_minio_running
    available_storages = ['local']
    for storage in SUPPORTED_STORAGES:
        if storage == 'local':
            continue
        elif storage == 'minio':
            if check_if_minio_running():
                available_storages.append('minio')
        else:
            raise ValueError(f"Unsupported {storage=}")
    return available_storages


def separate_number_and_chars(input_string):
    """Separates the number and characters from a string.
    Args:
        input_string: e.g. '1d'
    Returns:
        number_part: e.g. 1
        char_part: e.g. 'd'
    """
    # Regex pattern: (\d+) captures one or more digits, (\D+) captures one or more non-digits
    pattern = r'(\d+)(\D+)'
    
    match = re.match(pattern, input_string)
    
    if match:
        number_part = match.group(1)
        char_part = match.group(2)
        return number_part, char_part
    else:
        return None, None  # Return None if no match is found


def get_TZ_abbrev_and_UTC_offset(date: str, tz_identifier='US/Eastern'):
    '''Returns timezone abbreviation (e.g. EST, EDT) based on the timezone identifier.
    Useful when you want to determine if e.g. New York is in EST or EDT now.
    Args:
        date: e.g. 2023-11-11
        timezone: TZ identifier from IANA timezone database
    '''
    date = datetime.datetime.strptime(date, '%Y-%m-%d')
    timezone = pytz.timezone(tz_identifier)
    local_date = timezone.localize(date)  # attach timezone to the datetime object 
    return local_date.strftime('%Z%z')


def get_x_days_before_in_UTC(x=0) -> str:
    return (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=x)).strftime('%Y-%m-%d')


def get_dates_in_between(start_date: str | datetime.date, end_date: str | datetime.date) -> list[datetime.date]:
    if type(start_date) is str:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    if type(end_date) is str:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    date_delta = end_date - start_date
    return [start_date + datetime.timedelta(i) for i in range(date_delta.days + 1)]


def create_filename(pdt: str, date: str, file_extension: str) -> str:
    return pdt + '_' + date + file_extension


def extract_date_from_filename(filename: str) -> str | None:
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    match = re.search(date_pattern, filename)
    if match:
        date = match.group(0)
        return date


def rollback_date_range(rollback_period: str) -> tuple[datetime.date, datetime.date]:
    from pfund.datas.resolution import Resolution
    
    # check if rollback_period is a valid Resolution
    rollback_period = repr(Resolution(rollback_period))
    
    '''Returns start_date and end_date based on the rollback_period (e.g. '1w', '1M').'''
    def _nextmonth(year, month):
        if month == 12:
            return year+1, 1
        else:
            return year, month+1
    utcnow = datetime.datetime.now(tz=datetime.timezone.utc)
    period = int(rollback_period[:-1])
    if rollback_period.endswith('d'):
        timedelta = datetime.timedelta(days=period)
    elif rollback_period.endswith('w'):
        timedelta = datetime.timedelta(weeks=period)
    elif rollback_period.endswith('M'):
        year, month = utcnow.year, utcnow.month - period
        while month <= 0:
            month += 12  # Rollback to the previous year
            year -= 1
        total_days_in_month = 0
        while not (year == utcnow.year and month == utcnow.month):
            year, month = _nextmonth(year, month)
            _, days_in_month = calendar.monthrange(year, month)
            total_days_in_month += days_in_month
        timedelta = datetime.timedelta(days=total_days_in_month)
    elif rollback_period.endswith('y'):
        year = utcnow.year - period
        total_days_in_year = 0
        for yr in range(year, utcnow.year):
            total_days_in_year += 366 if calendar.isleap(yr) else 365
        timedelta = datetime.timedelta(days=total_days_in_year)
    else:
        raise ValueError(f"Unsupported {rollback_period=}")
    end_date = utcnow - datetime.timedelta(days=1)  # Previous day
    start_date = end_date - timedelta + datetime.timedelta(days=1)
    return start_date.date(), end_date.date()
