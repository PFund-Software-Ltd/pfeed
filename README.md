## Setup
```bash
poetry install
```

## Start Processing Historical Data
```
python main.py -m historical -p BTC_USDT_PERP -s bybit --no-minio
# or
from pfeed import bybit
bybit.run_historical(...)
```

## Start Streaming Live Data (supports PAPER trading)
```python
python main.py -e live -s bybit -m streaming
```
