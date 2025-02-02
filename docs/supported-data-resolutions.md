# Supported Data Resolutions

> NOTE: Orderbook data is NOT yet supported.

A resolution defines the granularity of financial data, determining how often data points are grouped or structured into time-based intervals. For example, a resolution of "1minute" means data is organized into 1-minute intervals.

- resolutions are **case-sensitive**, e.g. '1m' is different from '1M'.
- orderbook level must use **uppercase "L"**, e.g., "1q_L1", not "1q_l1".
- **orderbook data** is referred to as "**quote**" in pfeed.
- **public trade data** is referred to as "**tick**" in pfeed.

| Resolution | Description       |
| ---------- | ----------------- |
| 1q_L3      | orderbook level 3 |
| 1q_L2      | orderbook level 2 |
| 1q_L1      | orderbook level 1 |
| 1t         | 1 tick            |
| 1s         | 1 second          |
| 1m         | 1 minute          |
| 1d         | 1 day             |
| 1w         | 1 week            |
| 1M         | 1 month           |
| 1y         | 1 year            |

## Aliases
Resolutions are flexible, you don't need to be precise when specifying them. Here are some examples:

| Aliases                         | Resolution |
| ------------------------------- | ---------- |
| 'quote_L3', 'q_L3', '1quote_L3' | '1q_L3'    |
| '1min', '1minute'               | '1m'       |
| '1wk', '1week'                  | '1w'       |
| '1mon', '1month'                | '1M'       |


## Custom Time Periods
You can specify a custom period by changing the number before the unit. For example, "2m" represents 2-minute bars, while "5d" represents 5-day bars.

```{warning}
Choose time periods carefully. If the total data span (e.g., 1 day = 24 hours = 1440 minutes) is **not divisible by the selected period**, it may result in **incomplete data bars**. e.g. imagine you use '7minutes' to form data bars.
```