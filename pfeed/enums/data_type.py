from enum import StrEnum
    

class MarketDataType(StrEnum):
    QUOTE_L3 = q_L3 = 'QUOTE_L3'
    QUOTE_L2 = q_L2 = 'QUOTE_L2'
    QUOTE_L1 = q_L1 = 'QUOTE_L1'
    TICK = t = 'TICK'
    SECOND = s = 'SECOND'
    MINUTE = m = 'MINUTE'
    HOUR = h = 'HOUR'
    DAY = d = 'DAY'


class DataType(StrEnum):
    # market data types
    QUOTE_L3 = q_L3 = MarketDataType.QUOTE_L3
    QUOTE_L2 = q_L2 = MarketDataType.QUOTE_L2
    QUOTE_L1 = q_L1 = MarketDataType.QUOTE_L1
    TICK = t = MarketDataType.TICK
    SECOND = s = MarketDataType.SECOND
    MINUTE = m = MarketDataType.MINUTE
    HOUR = h = MarketDataType.HOUR
    DAY = d = MarketDataType.DAY
    # EXTEND: include e.g. news data, fundamental data etc.
    # fundamental data types
