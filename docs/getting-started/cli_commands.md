[MinIO]: https://min.io/

# CLI Commands

## Download Historical Data
```bash
# download data, default data type (dtype) is 'raw' data
pfeed download -d BYBIT -p BTC_USDT_PERP --start-date 2024-03-01 --end-date 2024-03-08

# download multiple products BTC_USDT_PERP and ETH_USDT_PERP and minute data
pfeed download -d BYBIT -p BTC_USDT_PERP -p ETH_USDT_PERP --dtype minute

# download all perpetuals data from bybit
pfeed download -d BYBIT --ptype PERP

# download all the data from bybit (CAUTION: your local machine probably won't have enough space for this!)
pfeed download -d BYBIT

# store data into MinIO (need to start MinIO by running `pfeed docker-compose up -d` first)
pfeed download -d BYBIT -p BTC_USDT_PERP --use-minio

# enable debug mode and turn off using Ray
pfeed download -d BYBIT -p BTC_USDT_PERP --debug --no-ray

# for more details, run:
pfeed download --help 
```

```{seealso}
See [Supported Data Sources](../supported-data-sources.md) to find out what data sources are supported.
```

## Configuration
```bash
# change the data storage location to your desired location
pfeed config --data-path your_desired_path

# list current configs
pfeed config --list

# for more config options, run:
pfeed config --help 
```

## Run PFeed's docker-compose.yml
PFeed provides a pre-configured `docker-compose.yml` file to simplify the setup of services like [MinIO]. This could be helpful for users unfamiliar with Docker. However, using this file is optional, and users are free to run their own Docker services if preferred.

To run PFeed's docker-compose.yml file:
```bash
# same as 'docker-compose', only difference is it has pointed to pfeed's docker-compose.yml file
pfeed docker-compose [COMMAND]

# e.g. start services
pfeed docker-compose up -d

# e.g. stop services
pfeed docker-compose down

# for more details, run:
pfeed docker-compose --help
```

```{tip}
If you have zero knowledge of docker, you only need to know that running the docker-compose file will start some services (like MinIO, a software for object storage) for you to use.
```

## Open Configuration Files
For convenience, you can open the config files directly from your CLI.
```bash
# open the config file with VS Code/Cursor AI
pfeed open --config-file

# open the logging.yml file with VS Code/Cursor AI
pfeed open --log-file

# open the docker-compose.yml file with VS Code/Cursor AI
pfeed open --docker-file

# open the logging.yml file with default editor
pfeed open --config-file -e
```
