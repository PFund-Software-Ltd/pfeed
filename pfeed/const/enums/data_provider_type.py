from enum import IntEnum


class DataProviderType(IntEnum):
    # higher value is better
    EXCHANGE = 3
    DISTRIBUTOR = 2
    VENDOR = 1  # data is not as raw as data provided from data distributors, e.g. firstrate_data
    AGGREGATOR = 0