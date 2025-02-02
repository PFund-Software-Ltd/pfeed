from enum import StrEnum
    

class DataType(StrEnum):
    # market data types
    QUOTE_L3 = q_L3 = 'QUOTE_L3'
    QUOTE_L2 = q_L2 = 'QUOTE_L2'
    QUOTE_L1 = q_L1 = 'QUOTE_L1'
    QUOTE = q = 'QUOTE'
    TICK = t = 'TICK'
    SECOND = s = 'SECOND'
    MINUTE = m = 'MINUTE'
    HOUR = h = 'HOUR'
    DAY = d = 'DAY'
    # EXTEND: include e.g. news data, fundamental data etc.
    # fundamental data types
    
    
class MarketDataType(StrEnum):
    QUOTE_L3 = q_L3 = DataType.QUOTE_L3
    QUOTE_L2 = q_L2 = DataType.QUOTE_L2
    QUOTE_L1 = q_L1 = DataType.QUOTE_L1
    QUOTE = q = DataType.QUOTE
    TICK = t = DataType.TICK
    SECOND = s = DataType.SECOND
    MINUTE = m = DataType.MINUTE
    HOUR = h = DataType.HOUR
    DAY = d = DataType.DAY
