[DuckLake]: https://ducklake.select/

# Overview

## What is `pfeed`
`pfeed` (/piː fiːd/) is the data engine for trading, serving as a pipeline between raw data sources and traders. It enables you to **download historical data**, **stream real-time data**, and **store cleaned data** in a **local data lake for quantitative analysis**, supporting both **batch processing** and **streaming** workflows through streamlined data collection, cleaning, transformation, and storage.

---

## Why use `pfeed`
You should use `pfeed` if you want to:
- Download or stream reliable, validated and **clean data** for research, backtesting, or live trading
- Get historical data (**dataframe**) or live data in standardized formats by just calling a **single** function
- **Own your data** by storing them locally, or to the cloud later
- Build a local data lakehouse (Parquet + [DuckLake]) on your own machine
- Interact with different kinds of data (including {abbr}`TradFi (Traditional Finance, e.g. Interactive Brokers)`, {abbr}`CeFi (Centralized Finance, e.g. Binance)` and {abbr}`DeFi (Decentralized Finance, e.g. Hyperliquid)`) using a **unified interface**

---

[alphafund]: https://github.com/PFund-Software-Ltd/alphafund
[pfund]: https://github.com/PFund-Software-Ltd/pfund
[pytrade]: https://pytrade.org

## Related Projects
- [alphafund] - AI-Powered Personal Hedge Fund. Be Your Own Fund Manager.
- [pfund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading
- [pytrade] - An Educational Hub for Trading with Python. Where Python Trading Begins


---

```{warning} Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This framework is intended for educational and research purposes only. It should not be used for real trading without understanding the risks involved. Trading in financial markets involves significant risk, and there is always the potential for loss. Your trading results may vary. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed on this platform.

The developers of this framework are not responsible for any financial losses incurred from using this software. This includes but not limited to losses resulting from inaccuracies in any financial data output by `pfeed`. Users should conduct their due diligence, verify the accuracy of any data produced by `pfeed`, and consult with a professional financial advisor before engaging in real trading activities.

```
