from typing import Literal

tSUPPORTED_PRODUCT_TYPES = Literal['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
tSUPPORTED_DATA_TYPES = Literal['raw_tick', 'tick', 'second', 'minute', 'hour', 'daily']