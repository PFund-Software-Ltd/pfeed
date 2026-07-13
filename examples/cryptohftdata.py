"""Download CryptoHFTData trades or resampled bars through PFeed."""

from __future__ import annotations

import argparse

import pfeed as pe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="binance_futures")
    parser.add_argument("--product", default="BTC_USDT_PERP")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--resolution", default="1m")
    parser.add_argument("--start-date", default="2024-07-13")
    parser.add_argument("--end-date", default="2024-07-13")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = pe.CryptoHFTData()
    result = client.market_feed.download(
        product=args.product,
        exchange=args.exchange,
        symbol=args.symbol,
        resolution=args.resolution,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(result.data.collect())


if __name__ == "__main__":
    main()
