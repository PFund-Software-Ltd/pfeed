from typing import Callable, Literal
import datetime
        
from pfund_kit.utils.temporal import get_utc_now, convert_to_date, get_yesterday


def lambda_with_name(name: str, lambda_func: Callable):
    lambda_func.__name__ = name
    return lambda_func


def get_ray_num_cpus(self) -> int:
    """Get the number of CPUs available in the Ray cluster."""
    import ray
    if not ray.is_initialized():
        raise RuntimeError('Ray must be initialized before getting the number of CPUs')
    cluster_resources = ray.cluster_resources()
    return int(cluster_resources.get('CPU', 0))


def determine_timestamp_integer_unit_and_scaling_factor(ts: float | int):
    """
    Determines the nearest integer timestamp unit and scaling factor for a given timestamp value.

    Problem: pd.to_datetime([1704067200.2353], unit='s') will return '2024-01-01 00:00:00.235300064',
    which is not what we want. We want to preserve all decimal digits.
    Solution: we can convert the timestamp to an integer using a scaling factor, in this case, in order to get 1704067200235300,
    we need to use this function to determine the integer unit is 'us' and the scaling factor is 10**6,
    so that we can do pd.to_datetime([1704067200235300], unit='us') and get '2024-01-01 00:00:00.235300'.

    Parameters:
        ts (float | int): The timestamp value to analyze.

    Returns:
        tuple[str, int]: A tuple containing:
            - str: The target timestamp unit ('s', 'ms', 'us', or 'ns')
            - int: The scaling factor to convert from current unit to target unit

    Examples:
        >>> determine_timestamp_integer_unit_and_scaling_factor(1704067200.2353)
        ('us', 1000000)
        >>> determine_timestamp_integer_unit_and_scaling_factor(1704067200235.312)
        ('us', 1000)
    """

    def infer_ts_unit(ts: float | int) -> str:
        """
        Infers the time unit of a timestamp based on its value.

        Parameters:
            ts (float or int): The timestamp value.

        Returns:
            str: The inferred unit ('s', 'ms', 'us', or 'ns').
        """
        if ts < 1e10:
            return "s"  # Seconds
        elif ts < 1e13:
            return "ms"  # Milliseconds
        elif ts < 1e16:
            return "us"  # Microseconds
        else:
            return "ns"  # Nanoseconds

    # Infer the unit of the timestamp
    unit = infer_ts_unit(ts)
    unit_multipliers = {"s": 0, "ms": 3, "us": 6, "ns": 9}
    unit_multiplier = unit_multipliers[unit]
    unit_multipliers_list = [0, 3, 6, 9]

    # Get the number of decimal places in the timestamp
    ts_str = str(ts)
    if "." in ts_str:
        decimal_part = ts_str.split(".")[1].rstrip("0")  # Remove trailing zeros
        num_decimal_places = len(decimal_part)
    else:
        num_decimal_places = 0

    # Determine the required unit multiplier to preserve all decimal digits
    required_multiplier = unit_multiplier + num_decimal_places
    possible_multipliers = [
        m for m in unit_multipliers_list if m >= required_multiplier
    ]
    if not possible_multipliers:
        raise ValueError("Timestamp precision exceeds nanoseconds.")

    target_unit_multiplier = possible_multipliers[0]
    target_unit = [
        k for k, v in unit_multipliers.items() if v == target_unit_multiplier
    ][0]

    # Calculate the scaling factor
    scaling_factor = 10 ** (target_unit_multiplier - unit_multiplier)

    return target_unit, scaling_factor


def rollback_date_range(
    rollback_period: str | Literal["ytd"],
) -> tuple[datetime.date, datetime.date]:
    """Returns start_date and end_date based on the rollback_period (e.g. '1d', 'ytd' (Year To Date))."""
    import calendar
    from pfund.datas.resolution import Resolution

    utcnow = get_utc_now()
    end_date = utcnow - datetime.timedelta(days=1)  # Previous day

    if rollback_period.lower() == "ytd":
        start_date = datetime.datetime(utcnow.year, 1, 1, tzinfo=datetime.timezone.utc)
    else:
        # check if rollback_period is a valid Resolution
        rollback_period: Resolution = Resolution(rollback_period)
        period = rollback_period.period
        if rollback_period.is_day():
            timedelta = datetime.timedelta(days=period)
        elif rollback_period.is_week():
            timedelta = datetime.timedelta(weeks=period)
        elif rollback_period.is_month():
            # Calculate target year/month
            year, month = utcnow.year, utcnow.month - period
            while month <= 0:
                month += 12
                year -= 1
            # Clamp day to the last day of target month if needed (e.g., Mar 31 → Feb 28)
            _, last_day_of_target_month = calendar.monthrange(year, month)
            day = min(utcnow.day, last_day_of_target_month)
            target_date = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)
            timedelta = utcnow - target_date
        elif rollback_period.is_year():
            year = utcnow.year - period
            total_days_in_year = 0
            for yr in range(year, utcnow.year):
                total_days_in_year += 366 if calendar.isleap(yr) else 365
            timedelta = datetime.timedelta(days=total_days_in_year)
        else:
            raise ValueError(f"Unsupported {rollback_period=}")
        start_date = end_date - timedelta + datetime.timedelta(days=1)
    return start_date.date(), end_date.date()


def parse_date_range(
    start_date: str | datetime.date,
    end_date: str | datetime.date,
    rollback_period: str | Literal["ytd"],
) -> tuple[datetime.date, datetime.date]:
    """Parse and standardize date range based on input parameters.

    Args:
        start_date: Start date string in YYYY-MM-DD format or datetime.date object.
            If not provided (None/empty), will be determined by rollback_period.
        end_date: End date string in YYYY-MM-DD format or datetime.date object.
            If not provided and start_date is provided, defaults to yesterday.
            If not provided and start_date is not provided, will be determined by rollback_period.
        rollback_period: Period to rollback from today if start_date is not provided.
            Can be a period string like '1d', '1w', '1m', '1y' etc.
            Or 'ytd' to use the start date of the current year.

    Returns:
        tuple[datetime.date, datetime.date]: Standardized (start_date, end_date)

    Raises:
        AssertionError: If start_date is after end_date
    """
    if start_date:
        start_date = convert_to_date(start_date)
        end_date = convert_to_date(end_date) if end_date else get_yesterday()
    else:
        start_date, end_date = rollback_date_range(rollback_period)
    assert start_date <= end_date, (
        f"start_date must be before end_date: {start_date} <= {end_date}"
    )
    return start_date, end_date
