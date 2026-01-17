from enum import IntEnum


class TimestampPrecision(IntEnum):
    SECOND = 0
    MILLISECOND = 3
    MICROSECOND = 6
    NANOSECOND = 9

    def __str__(self) -> str:
        return self.name
    
    __repr__ = __str__
