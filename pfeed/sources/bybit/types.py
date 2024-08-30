from typing import Literal

tSUPPORTED_PRODUCT_TYPES = Literal['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
tSUPPORTED_RAW_DATA_TYPES = Literal['raw_tick']
tSUPPORTED_DATA_TYPES = tSUPPORTED_RAW_DATA_TYPES | Literal['tick', 'second', 'minute', 'hour', 'daily']