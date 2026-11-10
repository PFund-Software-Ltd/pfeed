# Configuration

`pfeed` creates a config file at `~/.pfeed/config/` on first use. Your project's `.env` file, if any, is also loaded automatically; variables already set in your environment take precedence. To turn this off, set the environment variable `PFEED_LOAD_DOTENV=0`.

## View Config

```python
import pfeed as pe

config = pe.get_config()
```

Or from the command line:
```bash
pfeed config list   # print the config and the config file's path
pfeed config where  # print the config directory
```

```{note} Can I change the config directory?
:class: dropdown
Unlike the data and log paths, the config directory (`~/.pfeed/config/`) is fixed, since `pfeed` needs it to find the config file.
```

---

## Change Config

### In Python

Call `pe.configure()` **before** anything else. Only the options you pass are changed:
```python
pe.configure(
    data_path="path/to/data",
    log_path="path/to/logs",
    show_progress_bar=False,
)
```

Changes apply to the current session only. Pass `persist=True` to also save them to the config file.

| Option | Default | Description |
| --- | --- | --- |
| `data_path` | `~/.pfeed/data/` | where data is stored |
| `log_path` | `~/.pfeed/logs/` | where logs are written |
| `show_progress_bar` | `True` | whether progress bars are shown |
| `allow_timestamp_precision_loss` | `False` | whether IOs with coarser timestamps (e.g. microseconds) may truncate finer ones (e.g. nanoseconds) with a warning, instead of raising |
| `stream_latency_log_interval` | `None` | seconds between a stream's latency logs (at DEBUG level); `None` disables latency tracking |
| `stream_optimize_interval` | `1800` | seconds between `optimize()` calls on the table IOs (e.g. DuckLake) a stream writes to; `0` runs it once, when the streams end |

### From the Command Line

This changes the config file directly:
```bash
pfeed config set --data path/to/data --log path/to/logs
```

Or open the config file in your editor:
```bash
pfeed config open --config-file
```

To restore the defaults:
```bash
pfeed config reset --config-file
```

---

## Logging

`pfeed` logs follow the [Python logging config](https://docs.python.org/3/library/logging.config.html) format, defined in a `logging.yaml` file in the config directory.

To change it for the current session, call `pe.configure_logging()` before creating a feed. Your settings are merged on top of `logging.yaml`:
```python
# only print WARNING and above to the console
pe.configure_logging({"handlers": {"stream_handler": {"level": "WARNING"}}})

# or set every logger and handler to DEBUG
pe.configure_logging(debug=True)
```

To change it permanently, edit `logging.yaml`:
```bash
pfeed config open --logging-file
```
