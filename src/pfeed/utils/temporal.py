from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pfund.datas.resolution import Resolution

import datetime

from pfund_kit.utils.temporal import convert_to_date, get_utc_now, get_yesterday

__all__ = [
    "determine_timestamp_integer_unit_and_scaling_factor",
    "infer_ts_unit",
    "ns_to_seconds",
    "parse_date_range",
    "rollback_date_range",
    "seconds_to_ns",
]

NS_PER_SECOND = 1_000_000_000


def ns_to_seconds(ts_ns: int | None) -> float | None:
    """Convert pfeed's int-ns timestamps to float seconds.

    pfeed standardizes every timestamp to int ns since epoch, but pfund's `Bar`
    is a float-seconds API (resolution flooring, `end_ts = start_ts + secs - 0.001`,
    `datetime.fromtimestamp`). Use this when feeding ns timestamps into `Bar`.
    """
    return ts_ns / NS_PER_SECOND if ts_ns is not None else None


def seconds_to_ns(ts_s: float) -> int:
    """Convert pfund `Bar`'s float-second timestamps back to pfeed's int-ns contract.

    Inverse of `ns_to_seconds`. Bar boundaries are second-aligned, so the round-trip
    is exact for start_ts/end_ts; sub-ns drift on the update `ts` is inherent to
    routing through a seconds-based float API and is harmless.
    """
    return round(ts_s * NS_PER_SECOND)


def infer_ts_unit(ts: float | int) -> Literal["s", "ms", "us", "ns"]:
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
    target_unit = next(
        k for k, v in unit_multipliers.items() if v == target_unit_multiplier
    )

    # Calculate the scaling factor
    scaling_factor = 10 ** (target_unit_multiplier - unit_multiplier)

    return target_unit, scaling_factor


def rollback_date_range(
    rollback_period: Resolution | str | Literal["ytd"],
) -> tuple[datetime.date, datetime.date]:
    """Returns start_date and end_date based on the rollback_period (e.g. '1d', 'ytd' (Year To Date))."""
    import calendar

    from pfund.datas.resolution import Resolution

    utcnow = get_utc_now()
    end_date = utcnow - datetime.timedelta(days=1)  # Previous day

    if isinstance(rollback_period, str) and rollback_period.lower() == "ytd":
        start_date = datetime.datetime(utcnow.year, 1, 1, tzinfo=datetime.UTC)
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
            target_date = datetime.datetime(year, month, day, tzinfo=datetime.UTC)
            timedelta = utcnow - target_date
        elif rollback_period.is_year():
            year = utcnow.year - period
            # Clamp Feb 29 to Feb 28 in non-leap target year
            day = utcnow.day
            if utcnow.month == 2 and utcnow.day == 29 and not calendar.isleap(year):
                day = 28
            target_date = datetime.datetime(
                year, utcnow.month, day, tzinfo=datetime.UTC
            )
            timedelta = utcnow - target_date
        else:
            raise ValueError(f"Unsupported {rollback_period=}")
        start_date = end_date - timedelta + datetime.timedelta(days=1)
    return start_date.date(), end_date.date()


def parse_date_range(
    start_date: str | datetime.date | None = None,
    end_date: str | datetime.date | None = None,
    rollback_period: Resolution | str | Literal["ytd"] | None = None,
) -> tuple[datetime.date, datetime.date]:
    """Parse and standardize a date range.

    Either `start_date` or `rollback_period` must be provided:
    - If `start_date` is given, `end_date` defaults to yesterday.
    - If `rollback_period` is given, both dates are derived from it.

    Args:
        start_date: YYYY-MM-DD string or `datetime.date`.
        end_date: YYYY-MM-DD string or `datetime.date`.
        rollback_period: e.g. '1d', '1w', '1m', '1y', or 'ytd'.

    Returns:
        (start_date, end_date) as `datetime.date` objects.

    Raises:
        ValueError: if neither `start_date` nor `rollback_period` is given,
            if `end_date` is given without `start_date`, or if
            `start_date > end_date`.
    """
    if end_date and not start_date:
        raise ValueError(f"{end_date=} is set but start_date is not")
    if not start_date and not rollback_period:
        raise ValueError("either start_date or rollback_period must be provided")

    if start_date:
        start_date = convert_to_date(start_date)
        end_date = convert_to_date(end_date) if end_date else get_yesterday()
    else:
        assert rollback_period, (
            "rollback_period must be provided if start_date is not provided"
        )
        start_date, end_date = rollback_date_range(rollback_period)

    if start_date > end_date:
        raise ValueError(f"{start_date=} must be <= {end_date=}")
    return start_date, end_date
