# Config

## Set
```bash
# set your data path for storage
pfeed config set --data-path {your_data_path}

# set your .env
pfeed config set --env-file {your_env_file}
```

## Open
```bash
# open the config file (pfeed_config.yml)
pfeed config open --config-file

# open the .env file
pfeed config open --env-file
```

## Where
```bash
# see where your config is
pfeed config where
```

## List
```bash
# list current configs
pfeed config list
```

## Reset
```bash
# reset all config files, including logging.yml, docker-compose.yml etc.
pfeed config reset

# reset configs, i.e. pfeed_config.yml
pfeed config reset --name config
``` 
