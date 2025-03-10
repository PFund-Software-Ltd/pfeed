from enum import IntEnum


class DataAccessType(IntEnum):
    # higher value is better
    FREE = 4
    FREE_TIER = 3  # e.g. polygon
    # NOTE: PAID implicity means paid by subscription, coz there's not one-off payment for data access
    PAID = 2  # e.g. firstrate_data, still better than PAID_BY_TIER because once paid, access is unlimited. 
    PAID_BY_TIER = 1  # no free tier, paid by tier, each tier has different limitations
    PAID_BY_USAGE = 0  # e.g. databento
