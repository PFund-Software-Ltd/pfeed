import re
import datetime
import calendar
import pytz


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


def get_dates_in_between(start_date: str, end_date: str) -> list[str]:
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    date_delta = end_date - start_date
    return [str(start_date + datetime.timedelta(i)) for i in range(date_delta.days + 1)]


def create_filename(pdt: str, date: str, file_extension: str) -> str:
    return pdt + '_' + date + file_extension


def extract_date_from_filename(filename: str) -> str | None:
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    match = re.search(date_pattern, filename)
    if match:
        date = match.group(0)
        return date


def rollback_date_range(rollback_period: str) -> tuple[str, str]:
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
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
