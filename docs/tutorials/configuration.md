---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Configuration
By default, an auto-generated config file is created in the user's system config directory. 

## Get Config

### 1. get_config()
You can get the current config in runtime by calling `pe.get_config()`.
```python
import pfeed as pe
config = pe.get_config()
```

### 2. CLI Command
You can also get the current config by using the CLI command `pfeed config --list`.

```{note}
`config_file_path` shown in `pfeed config --list` is the path to the config file, which is *FIXED* and *CANNOT* be changed.
```

## Change Config
### 1. configure()
The **recommended** way to change the configuration is to use the `configure()` function. It will only change the config in runtime and should be called **before any other functions** in the library.
```python
config = pe.configure(
    data_path='your_data_path',  # path to store data
    log_path='your_log_path',  # path to store logs
    debug=True,  # if True, will print debug information
    env_file_path='your_env_file_path',  # path to the .env file
)
```

#### - Example: Change Logging Level
This sets the logging level of the stream handler to WARNING
so that only WARNING, ERROR, and CRITICAL messages are printed to the console.
```python
pe.configure(
    logging_config={'handlers': {'stream_handler': {'level': 'WARNING'}}},
)
```

#### - Example: Load Logging Config File
You can write all the logging config in a yaml file and load it.
```python
pe.configure(
    logging_config_file_path='path/to/your/logging.yml',
)
```

```{tip}
`pfeed`'s logging_config follows the [python logging config](https://docs.python.org/3/library/logging.config.html) format. 
```

### 2. CLI Command
You can change the config by using the CLI command `pfeed config`.
This will change the config file in the user's system config directory.
```bash
pfeed config --data-path your_data_path --log-path your_log_path --debug True --env-file-path your_env_file_path
```

### 3. Edit Config File
You can also directly edit the config file manually.
```bash
pfeed open --config-file
```

```{seealso}
For more details, see the [CLI commands](../getting-started/cli_commands.md) documentation.
```