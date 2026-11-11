## [0.0.24](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.23...v0.0.24) - 2026-11-11

### 🚀 Features

- Add PFEED_LOAD_DOTENV env var to opt out of loading .env
- Add pfeed.testing e2e suite for source plugins, and integration tests

### 🐛 Bug Fixes

- Resolve ty errors and ruff lint/format issues
- Keep the clicked FAQ question highlighted in the sidebar
- Collect batch results in Ray workers so retrieve works with DuckLakeIO
- Only force-cancel Ray stream workers that did not stop in time
- Accept Windows drive-letter paths as local in ParquetIO and TableIO
- Stop IcebergIO.vacuum() deleting in-use files on Windows
- Build IcebergIO's warehouse URI with forward slashes on Windows
- Report native paths from IcebergIO.vacuum() on Windows

### 📚 Documentation

- Doc sketch
- New logo, fix the API Reference nav link and bump afterpython to 0.4.7
- Write pipeline mode, streaming and integrations docs, merge supported pages and bump pfund-kit to 0.1.25
- Sync README quick start with the docs, add DuckLake stream storage and replay examples
- Expand the README table of contents by default
- Replace outdated DeltaLake mention in README with Parquet + DuckLake
- Note automatic resampling in the quickstart download section
- Add a notebook (Jupyter/Marimo) note on running streams to the streaming docs
- Add FAQs with a nav link and bump afterpython to 0.4.8
- Update keywords across MyST configurations
- Tell Windows users to enable long paths

### 🚜 Refactor

- Drop bytes from DataFlowResult/RunResult data types

### 🎨 Styling

- Ruff format and lint the whole repo

### 🧪 Testing

- Skip pandas transformation test when pandas is not installed
- Raise the Prefect test server startup timeout for busy CI runners
- Set pytest-asyncio's default fixture loop scope to silence its warning

### ⚙️ Miscellaneous Tasks

- Re-initialize afterpython with ap init
- Restore pfeed-specific afterpython config, README.py and branding assets
- Move ruff and ty config into afterpython/
- Run worktree setup as a prek post-checkout hook
- Add binance and okx sources
- Link .venv to pixi's default environment on activation
- Disable ty always_run in pre commit
- Bump afterpython to 0.4.5
- Reload website pages during ap dev when static/*.json data changes
- Redesign the website FAQ page with an "On this page" sidebar
- Rename outdated ty rule names in ignore comments
- Update package keywords
- Sync the website with project-website-template 0.5.0
- Add temporary diagnostics for crashing Ray test workers
- Remove temporary Ray crash diagnostics
- Run the pixi test matrix on windows-latest too
- Skip Python 3.15 on Windows until its dependencies ship wheels

### 💼 Other

- Depend on pfeed instead of pfeed[bybit]
- Add afterpython to the dev dependency group for the website deploy
- Bump afterpython to 0.4.7 in the dev dependency group
- Add py313-windows-vm environment for Windows ARM VM testing
- Bump afterpython to 0.5.0
- Add a test-vm task for the Windows ARM VM
- Skip the import time test in the Windows ARM VM

## [0.0.23](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.22...v0.0.23) - 2026-11-07

### 💼 Other

- Version 0.0.22 → 0.0.23

## [0.0.22](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.21...v0.0.22) - 2026-11-07

### 💼 Other

- Version 0.0.21 → 0.0.22

## [0.0.21](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.20...v0.0.21) - 2026-11-07

### 🚀 Features

- Add FXMacroData announcement feed
- *(io)* Handle schema drift in IO contract and ParquetIO
- *(io)* Add DuckLakeIO with a SQLite catalog and `pfeed ducklake` maintenance CLI
- *(io)* Add TableIO base unifying DuckLakeIO, DeltaLakeIO and IcebergIO
- *(io)* Support DatePartition in DuckLakeIO, DeltaLakeIO and IcebergIO
- *(io)* Write DuckLake parquet files with zstd
- *(io)* Append keeps existing partition metadata; add generic stream Sink
- *(streaming)* Store streamed market data through the data handler
- Add pe.get_feed as the public way to create a feed
- *(logging)* Derive a logger per installed data source
- Declare feed capabilities in source metadata
- Export MarketFeed from pfeed for subclassing
- *(source)* Accept str asset types in SourceMetadata.data_categories
- *(config)* Accept Path in configure()

### 🐛 Bug Fixes

- Use to_pascal_case for class name lookup
- Infer Bybit CSV schema from all rows
- *(io)* Validate all partition paths before writing; add ParquetIO tests
- Drop leftover DataSource enum usages
- *(market)* Resample window ignores the resolution period
- *(market)* Only clean downloaded data when the data layer is not raw
- *(ducklake_io)* Use fetchall() on relations for duckdb 2

### 📚 Documentation

- Mark Bybit USDC futures as deprecated
- Clean up download() and retrieve() docstrings
- Note third-party plugin imports are for IDE hints only, never install them

### 🚜 Refactor

- Make download dataflow_per_date a TimeBasedFeed class var
- Drop symbol from base MarketFeed.download
- Remove passthrough Bybit download override
- [**breaking**] Make MarketFeed.download params keyword-only after resolution
- Align retrieve() signature with download()
- Drop dead date assert in download()
- Move io/ to io_OLD/ to make room for the new IO contract
- New BaseIO contract (DatasetKey, per-partition metadata) and ParquetIO
- Keyword-only IO args, write(partitions=...), JSON-safe metadata; add IO contract tests
- Rebuild data handlers on the IO layer and drop legacy storage enums
- Wire feeds and dataflows to data handlers on the IO layer
- Move base source and data client to the package root, retire old sinks
- *(io)* Make DatePartition logical only in the table IOs
- Remove feeds draft (not in use)
- Drop yfinance support before 0.1.0
- Replace source mixins with a DataSource class attribute
- Move Bybit source out to the pfeed-bybit plugin
- Replace DataSource enum with an entry-point registry
- Replace create_feed with registry.get_feed
- Resolve data source clients via entry points, drop aliases
- Drop ANNOUNCEMENT_DATA category, type-only Callable import
- Merge DataProviderSource into BaseSource, move API concerns out of the base
- Store data source as a name string in data models
- Merge DataClient into BaseSource, making the source the only entry point
- Declare capabilities on feeds and check their requirements at class definition
- Move fxmcrodata and cryptohftdata examples into their source folders
- Rename data_origin to url in source metadata
- Add streaming_ to make function names more explicit
- Remove _create_data_model_from_request, support request.to_data_model()
- Lowercase overridable feed class attributes, declare MarketFeed capabilities
- Make quote handling explicit in get_supported_resolutions, add unit test
- Simplify create_data_model, add unit tests with bybit_product fixture
- Make create_data_model a classmethod, make feeds generic over their source
- Group code by domain into base/, market/, news/
- Use MarketFeed.Capability as the extract_type of market requests
- Gate feed verbs with a requires(capability) decorator
- Simplify _standardize_dates and its rollback_period='max' hook
- Move arrow_schema_builder from utils/ to schemas/
- *(requests)* Require concrete types and a non-None data_resolution
- *(feeds)* Make request and data model types feed type parameters
- Remove _etl module, name cleaned-data columns in schemas
- Move compression helpers from enums to utils
- *(schemas)* Tighten market data schema checks and add tests
- *(feed)* Checkpoint cleanup of _create_batch_dataflows
- *(feed)* Per-feed default transformations, abstract request hooks
- *(feed)* Finalize at run time, self-contained prefect export
- *(prefect)* Per-run use_prefect, named tasks, subflows under Ray
- *(faucet)* Retry batch extraction per source, rename METADATA to metadata
- *(feed)* Replace Capability enum with nested Requests class
- *(utils)* Remove FilePath/HfPath and cloudpathlib dependency
- Reserve pe.<Source> for official sources, move pfeed-bybit into sources/
- *(feed)* Harden batch dataflow runs and Ray scheduling
- *(feed)* Requests carry the source object and retry policy, harden retrieve probing
- *(cli)* [**breaking**] Remove `data` command
- Import Environment from pfund_blocks
- Move ray utils to pfund_kit
- Import Resolution and Timeframe from pfund_blocks

### 🧪 Testing

- Move bybit fixture to conftest, drop dead fixtures
- Assert Bybit download e2e output with polars
- Remove Bybit mixed e2e tests without bybit_mixed fixture
- Fix expected bar columns in Bybit download e2e
- Expect no symbol column in Bybit spot ticks
- Deprecate Bybit USDC future e2e param
- Round-trip Bybit e2e download through local parquet storage
- Remove outdated tests, move source tests under tests/unit/sources/
- Guard import time of pfeed
- Cover registry with fake entry points
- *(handler)* Add unit tests for base, time-based and market data handlers
- *(dataflow)* Add unit tests for dataflow, faucet and result
- *(feed)* Add BaseFeed unit tests, mirror src layout in tests/unit
- *(market)* Add download() and default transformation unit tests
- *(market)* Add retrieve() unit tests
- *(integration)* Add fake-vendor integration tests for MarketFeed

### ⚙️ Miscellaneous Tasks

- Sync pixi.lock with local pfund
- Remove the *_OLD code from the package
- Remove unused news feed request and stale tests
- Move source-specific tests next to their placeholder sources
- Bump ty to 0.0.85
- *(tests)* Remove empty test_etl.py, sort imports in test_registry
- Remove leftover news ETL module, sort utils imports
- Add just module to run pfeed-bybit from pfeed

### 💼 Other

- Replace zstandard with pyarrow's zstd, add pyiceberg as optional dep
- Lint tests with ruff
- Upgrade pytest-mock, pytest-recording, add pytest-markdown-docs
- Move deltalake and pyiceberg from extras to pixi dev deps
- Make yfinance a core dependency, drop the pandas extra, enable YAHOO_FINANCE
- Update deps
- Update pixi lock
- Drop Python 3.12, require >=3.13
- Support python 3.15, bump dependency versions
- Version 0.0.20 → 0.0.21

## [0.0.20](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.19...v0.0.20) - 2026-09-12

### 🚀 Features

- Add AlphaFundFeed
- Support partial AlphaFund chat updates

### 🐛 Bug Fixes

- Make schema_name in sqlite optional
- Fund and agent feed missing params
- Add use_prefect to config to avoid implicitly using prefect when it is running

### 🚜 Refactor

- Rename _sinks/ to sinks/, _io/ to io/
- Clena up alphafund feeds
- Rewrite alphafund feeds

### ⚙️ Miscellaneous Tasks

- Disable ruff-check in pre-commit temporarily
- Disable ruff check in ci.yml temporarily

### 💼 Other

- *(deps)* Bump actions/setup-python in the github-actions group
- Version 0.0.19 → 0.0.20

## [0.0.19](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.18...v0.0.19) - 2026-07-22

### 🚀 Features

- Add SQLite

### 💼 Other

- Version 0.0.18 → 0.0.19

## [0.0.18](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.17...v0.0.18) - 2026-07-16

### 🚜 Refactor

- Deltalake cli command so that it can be reused by pfund

### 💼 Other

- Version 0.0.17 → 0.0.18

## [0.0.17](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.16...v0.0.17) - 2026-07-15

### 🐛 Bug Fixes

- Update bybit after pfund's venue-based refactoring, update pfund component feed
- Assert env to be backtest in checkpoint artifact
- Store pfund component by component_id, move fields from data model to ComponentMetadata
- Component trading_df.delta should be overwritten in backtest

### 🚜 Refactor

- Rename run_name to run_id in component feed

### 💼 Other

- Update pixi.lock after version bump
- *(deps)* Bump the github-actions group across 1 directory with 2 updates
- Version 0.0.16 → 0.0.17
- Version 0.0.16 → 0.0.17

## [0.0.16](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.15...v0.0.16) - 2026-06-16

### 🚀 Features

- (wip) finalize pfund component feed draft

### 🚜 Refactor

- Clean up component feed and remove file backend in storage config

### 💼 Other

- Update pixi.lock after version bump
- Update pfund version
- Version 0.0.15 → 0.0.16

## [0.0.15](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.14...v0.0.15) - 2026-06-04

### 🐛 Bug Fixes

- Convert ns <-> s between pfeed's streaming msg and pfund's bar data

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.14 → 0.0.15

## [0.0.14](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.13...v0.0.14) - 2026-06-04

### 🚀 Features

- Add huggingface storage
- Update duckdb timestamp precision to ns

### 🐛 Bug Fixes

- Make _finalize_run iterate over every queued request (not just current request)
- Msg "time" unit in yahoo finance streaming
- Batch data schema precision, "date" always use ns and "side" use pl.Int8
- Duckdb creates db path wrongly
- Replaying should skip trnasformation and loading, the (TL) part in ETL
- Replaying should not add sink

### 🚜 Refactor

- Clean up download + stream cli (NOT DONE)

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.13 → 0.0.14

## [0.0.13](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.12...v0.0.13) - 2026-06-02

### 🐛 Bug Fixes

- ImportError when deltalake is not installed

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.12 → 0.0.13

## [0.0.12](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.11...v0.0.12) - 2026-05-27

### 🐛 Bug Fixes

- Lower polars version, its 1.41.0 is yanked

### 📚 Documentation

- Update README
- Update README
- Update README.md

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.11 → 0.0.12

## [0.0.11](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.10...v0.0.11) - 2026-05-26

### 🐛 Bug Fixes

- Handle bybit spot data schema with strict=False in df.rename

### 📚 Documentation

- Add retrieve to README.py

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.10 → 0.0.11

## [0.0.10](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.9...v0.0.10) - 2026-05-26

### 💼 Other

- Update pixi.lock after version bump
- Remove extras in cloudpathlib and make it available for WASM
- Version 0.0.9 → 0.0.10

## [0.0.9](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.8...v0.0.9) - 2026-05-26

### 🐛 Bug Fixes

- Circular import in market data schema

### 📚 Documentation

- Update README.py
- Update README.py

### 💼 Other

- Update pixi.lock after version bump
- Version 0.0.8 → 0.0.9

## [0.0.8](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.7...v0.0.8) - 2026-05-25

### 📚 Documentation

- Update README.py

### 💼 Other

- Update pixi.lock after version bump
- Exclude cloudpathlib from WASM
- Version 0.0.7 → 0.0.8

## [0.0.7](https://github.com/PFund-Software-Ltd/pfeed/compare/v0.0.6...v0.0.7) - 2026-05-25

### 📚 Documentation

- Fix logo and favicon in project website
- Add README.py stub
- Update README.py stub to use sandbox

### ⚙️ Miscellaneous Tasks

- Install marimo in deploy workflow for README.py

### 💼 Other

- Update pixi.lock after version bump
- Update versions
- Version 0.0.6 → 0.0.7

## [0.0.6] - 2026-05-25

### 🚀 Features

- Add --debug to skip printing logs to console when downloading data
- Get_historical_data supports local data resampling
- Add polars support
- Update minio
- Update yahoo finance feed and README
- Global config
- Add file format in utils for reading different types of raw data
- Use Ray to speed up in get_historical_data()
- Add command "pfeed doc" + use jupyter nbconvert to execute jupyter notebooks
- Add terminal UI
- Add metadata, add raw_level to metadata
- Allow extract_data with less strict raw_level
- Bind channels to dataflows
- Replay historical data via stream()

### 🐛 Bug Fixes

- Bybit feed get_historical_data
- Override current env variables in load_dotenv()
- Minio bug, ray segmentation fault
- Resolution='2d' bug when getting historical data using bybit feed
- 'first' column resample bug
- Dropna subset for tick data
- Wrong data_path in download command
- Dtypes bug
- Config command
- Create parent directories in save_config() in config command
- Resolution bug in bybit get_historical_data
- Bugs in download
- Paths
- Doc buliding
- Click cli latency
- Import error
- Logging bug that replaces the existing logging config
- Logging config
- Data feed resample bug
- Fix ray batch size, bybit reverse ts, minio slow loading
- Tqdm bar when downloading data
- Only organize columns for non-raw data
- Yahoo finance feed data_tool name typo
- Supported product types in download command
- Minio storage s3:// pathlib error
- Download bug due to raw level change
- Remove __index_level_0__ error by setting preserve_index=False in pyarrow
- Remove per-date cache storage
- Download bybit with raw_level "original"
- Set dask .compute(scheduler='synchronous')
- Resample_data should not change the original df
- Resampling should happen in both _get_historical_data_from_storage and _get_historical_data_from_source
- Remove TypeVar
- Search local data across diff resolutions and storages
- Update resolution in resampled df
- Convert yahoo finance volume column from int to float to comply with schema
- Bugs for get_historical_data aftger refactoring
- Incorrect stream handler log level in ray task
- Bybit + yahoo finance download error
- Bug after removing metadata
- Update duckdb storage after adding data_layer and data_domain
- Prefect dataflow
- Pfund enums import
- Pyarrow bug in pandas read_parquet, use polars to workaround
- Skip passing in empty df to polars scan_parquet
- Pandera's stupid validate_index_reset
- Retrieve df with the wrong resolution in curated layer
- Storage bugs in __new__
- Minio storage read/write
- Memory pointer from external source (e.g, FFI) is not aligned with the specified scalar type.
- Reading empty buffer.arrow
- Disallow default transformations when data layer is raw
- Dask df is also awaitable, use iscoroutine()
- Add ray cleanup to tests
- Bugs in data models
- Wrong logger names, add back prefix "pfeed."
- Bugs in metadata model
- Running prefect dataflow
- DeltaStorageMixin after major refactoring that added IOs
- Respect data model's date range in TimeBasedDataHandler's read()
- Progress update when using ray
- Unpredictable iteration order in CacheStorage
- ParquetIO recursion bug for calling its own exists()
- Stringify FilePath objects in ParquetIO read
- Pass in config.cache_path to data_path in CacheStorage
- Retrieve() after IOs refactoring
- Use min() in get_lowest_resolution, not max()
- Rollback_date_range when rollback period is 1 month
- Add back validate_default=True in StorageConfig's data_path
- Copy data model, do NOT share the same object between dataflow and faucet
- Auto_load should only be triggered by download request
- Download in yahoo finance market feed
- Remove outdated _write_batch
- Support multiple products in feed.stream() inside a for loop
- Support multiple requests so request.product is correct in default transformations for streaming
- Data config only used for bar resolution in stream()
- Add back kw_only=True to MarketDataMessage
- Add back data source to create_faucet
- Missing data domain
- Convert to enums in io config
- Retrive()
- Resampling and raw data layer storage
- Partitioning streaming data in deltalake
- Circular import in DataSink
- Pfeed.png path

### 📚 Documentation

- Jupyter book init
- Update
- Update README.md
- Update
- Update
- Update
- Add skeleton
- Add draft
- Update
- Update
- Fix python dependency
- Fix python version
- Update
- Update
- Update theme
- Debug
- Update
- Fix dark theme
- Update README
- Update README
- Migrate to myst
- Update README
- Fix for using myst
- Deploy myst doc
- Disable jupyter-book deploy
- Update concepts
- Update README
- Add medallion architecture
- Update
- Update cli commands
- Update README

### ⚡ Performance

- Improve minio load time
- Cache minio server check
- Use ray to speed up resample_data
- Implement in-memory buffer, buffer.arrow before writing to delta lake to speed up writing data

### 🚜 Refactor

- Download logic, filepath, datastore
- Separate raw data and tick data, and consolidate data types to be "bytes" at etl.py level
- Make etl script generic
- Yahoo finance feed
- Clean up feeds, download and fix import latency
- Change filepath to env/data_source/trading_venue/ptype/pdt/dtype/year/month/filename
- Clean up get_historical_data and etl logic
- Remove ray in get_historical_data
- Remove batch size in download
- Remove part size in minio
- Fix config, add config subcommands
- Rewrite the core to introduce dataflows, support more data types other than just trade data, integrate with prefect
- Add ios, data handlers, delta lake, data layers, data domains for modularity
- Update for pfund's product and asset type changes
- Add mixin to create data source
- Typing.py -> _typing.py
- RestApi → RESTfulAPI, WebsocketApi → WebSocketAPI
- Move utils.py to utils/__init__.py, rename _typing.py to typing.py, _logging to logging
- Remove auto_transform, move data_model_class to feed's level
- Separate IOs and storages into file io, table io and db io
- Remove _eager_run* functions in feed
- Add load request and finalize transformations in load()
- Remove io_options, add io_config
- Clean up storages and IOs

### 🎨 Styling

- Change text

### 🧪 Testing

- Imports
- Clean up
- Add bybit tests
- Yahoo finance market feed download()
- Add bybit test for download() and retrieve()
- Skip tests (not ready) for now

### ⚙️ Miscellaneous Tasks

- Update doc workflow
- Test release.yml
- Bump version to 0.0.1.dev13
- Bump version to 0.0.1.dev14
- Prepare for v0.0.1
- Bump to 0.0.2.dev1
- Bump to 0.0.2.dev2
- Fix dependency versions for WASM and bump to 0.0.2.dev3
- Bump to 0.0.2
- Bump to 0.0.3
- Bump to 0.0.4
- Bump to 0.0.5
- Migrate from poetry to uv and pixi
- Update versions in pyproject and pixi

### 💼 Other

- Bybit eda notebook
- README.md
- README.md
- README.md
- Write config's data path to environment variable if not set
- ConfigHandler.load_config()
- Add config_file_path to the output of "pfeed config --list"
- ConfigHandler.load_config()
- Click.echo data path
- Add core types (e.g. tDataframe), and data engines (e.g. dask, spark)
- Add env and identify delta table in "pfeed data list" command
- Update pixi lock file
- Version 0.0.5 → 0.0.6
