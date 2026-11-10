# CLI Commands

| Command | Description |
| --- | --- |
| [`pfeed download`](#pfeed-download) | Download historical market data and store it |
| [`pfeed stream`](#pfeed-stream) | Stream live market data |
| [`pfeed data`](#pfeed-data) | Explore stored data in a notebook |
| [`pfeed ducklake`](#pfeed-ducklake) | Maintain the DuckLake tables |
| [`pfeed config`](#pfeed-config) | View and change the configuration |
| [`pfeed remove`](#pfeed-remove) | Remove data and logs |
| `pfeed tui` | Open the terminal UI |

Run `pfeed <command> --help` for the full details of any command.

---

(pfeed-download)=
## `pfeed download`

```text
pfeed download SOURCE PRODUCT RESOLUTION [OPTIONS]
```

| Option | Description | Default |
| --- | --- | --- |
| `--start` | First date (inclusive), `YYYY-MM-DD` | |
| `--end` | Last date (inclusive), `YYYY-MM-DD` | yesterday (UTC) |
| `--rollback` | How far to roll back from today (UTC) when `--start` is not given, e.g. `7d`, `ytd`, `max` | `1d` |
| `--layer` | Data layer to store the data in: `raw` or `cleaned` | `cleaned` |
| `--io` | Where and how to store the data: `parquet` or `ducklake` | `parquet` |
| `--path` | Directory to store the data under | `pfeed`'s data path |
| `--origin` | Sub-label for data from the same source but different origins | |

```bash
pfeed download bybit BTC_USDT_PERP 1m --start 2025-01-01 --end 2025-01-31
pfeed download bybit BTC_USDT_PERP 1t --rollback 7d --io ducklake
```

```{tip}
Product specifications are passed as extra options, e.g. `--expiration 2025-12-26` for futures. Leave them out to get an error listing the required ones.
```

---

(pfeed-stream)=
## `pfeed stream`

```text
pfeed stream SOURCE PRODUCT RESOLUTION [OPTIONS]
```

| Option | Description | Default |
| --- | --- | --- |
| `--io` | Where and how to store the streamed data: `parquet` or `ducklake` | not stored |
| `--path` | Directory to store the data under (with `--io`) | `pfeed`'s data path |
| `--env` | Trading environment: `paper` or `live` | `live` |
| `--origin` | Sub-label for data from the same source but different origins | |
| `--flush-interval` | Seconds between writes of the buffered data (with `--io`) | `100` |
| `--incremental-bars` | Also store the updates of a bar before it closes (with `--io`) | |
| `--quiet` | Do not print the streamed data | |

```bash
pfeed stream bybit BTC_USDT_PERP 1m
pfeed stream bybit BTC_USDT_PERP 1t --io ducklake --quiet
```

---

(pfeed-data)=
## `pfeed data`

```text
pfeed data PATH [OPTIONS]
```

| Option | Description | Default |
| --- | --- | --- |
| `--keep` | Keep the notebook after exit | deleted on exit |
| `--port` | Port of the data explorer | a free port |
| `--data-path` | Override the data path | `pfeed`'s data path |

```bash
pfeed data ducklake                        # the DuckLake tables stored by pfeed
pfeed data parquet                         # the Parquet files stored by pfeed
pfeed data ~/Downloads/trades.parquet --keep
```

```{note}
`PATH` can be a Parquet, DuckDB, DuckLake or SQLite file, a folder of Parquet files, or a DuckLake folder; the type is detected automatically. A relative `PATH` is resolved against the data path, so `ducklake` above means `~/.pfeed/data/ducklake`.
```

---

(pfeed-ducklake)=
## `pfeed ducklake`

```text
pfeed ducklake [OPTIONS] COMMAND
```

| Option | Description | Default |
| --- | --- | --- |
| `--base-path` | DuckLake directory to maintain | `<data_path>/ducklake` |

| Command | Description |
| --- | --- |
| `optimize` | Compact all tables into fewer, larger Parquet files to speed up reads |
| `vacuum` | Delete history older than the retention period (`--retention-hours`, default `168`). Dry run by default; add `--no-dry-run` (`-n`) to actually delete |

```bash
pfeed ducklake optimize
pfeed ducklake vacuum --no-dry-run
```

```{tip}
Run `vacuum` after `optimize`: `optimize` only adds files, and the replaced ones stay on disk until `vacuum` deletes them.
```

---

(pfeed-config)=
## `pfeed config`

| Command | Description |
| --- | --- |
| `list` | Print the config and the config file's path |
| `where` | Print the config directory |
| `set` | Change the data path (`--data`) or log path (`--log`) |
| `open` | Open the config file (`-c`) or `logging.yaml` (`-l`) in your editor |
| `reset` | Reset the config file (`-c`) or `logging.yaml` (`-l`) to defaults, all of them if no flag is given |

See [Configuration](config.md) for details.

---

(pfeed-remove)=
## `pfeed remove`

Alias: `pfeed rm`

| Command | Description |
| --- | --- |
| `data [TARGET]` | Remove a file/folder under the data path, or empty it if no `TARGET` is given |
| `log [TARGET]` | Remove a file/folder under the log path, or empty it if no `TARGET` is given |

Asks for confirmation first; add `-y` to skip it.

```bash
pfeed rm data some_folder
```

---

## `pfund-shell`

`pfeed` comes with `pfund-shell`, an interactive shell that brings together the commands of every installed package in the PFund ecosystem (e.g. `pfeed`, `pfund`) in one place, with autocompletion:

```bash
pfund-shell
```
