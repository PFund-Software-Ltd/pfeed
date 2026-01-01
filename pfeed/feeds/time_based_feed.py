from __future__ import annotations
from typing import TYPE_CHECKING, TypedDict, Literal, Callable, Awaitable, Any, ClassVar
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfeed._io.base_io import StorageMetadata
    from pfeed.data_handlers.time_based_data_handler import TimeBasedStorageMetadata
    from pfeed.typing import tStorage, tDataLayer, GenericFrame, GenericFrameOrNone
    from pfeed.flows.dataflow import DataFlow, FlowResult
    from pfeed.flows.faucet import Faucet
    from pfeed.enums import StreamMode
    class TimeBasedFeedMetadata(TypedDict, total=True):
        missing_dates: list[datetime.date]
    GenericFrameOrNoneWithMetadata = tuple[GenericFrameOrNone, TimeBasedFeedMetadata]

import datetime
from pprint import pformat

from pfund import print_warning
from pfeed.utils.dataframe import is_empty_dataframe
from pfeed.enums import ExtractType
from pfeed.feeds.base_feed import BaseFeed, clear_subflows
from pfeed.streaming_settings import StreamingSettings
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


__all__ = ["TimeBasedFeed"]


class TimeBasedFeed(BaseFeed):
    data_model_class: ClassVar[type[TimeBasedDataModel]]
    
    def _standardize_dates(self, start_date: str | datetime.date, end_date: str | datetime.date, rollback_period: str | Literal['ytd', 'max']) -> tuple[datetime.date, datetime.date]:
        '''Standardize start_date and end_date based on input parameters.

        Args:
            start_date: Start date string in YYYY-MM-DD format.
                If not provided, will be determined by rollback_period.
            end_date: End date string in YYYY-MM-DD format.
                If not provided and start_date is provided, defaults to yesterday.
                If not provided and start_date is not provided, will be determined by rollback_period.
            rollback_period: Period to rollback from today if start_date is not provided.
                Can be a period string like '1d', '1w', '1m', '1y' etc.
                Or 'ytd' to use the start date of the current year.
                Or 'max' to use data source's start_date if available.

        Returns:
            tuple[datetime.date, datetime.date]: Standardized (start_date, end_date)

        Raises:
            ValueError: If rollback_period='max' but data source has no start_date attribute
        '''
        if rollback_period == 'max' and not start_date:
            if self.data_source.start_date:
                start_date = self.data_source.start_date
            else:
                raise ValueError(f'{self.name} {rollback_period=} is not supported')        
        start_date, end_date = self._parse_date_range(start_date, end_date, rollback_period)
        return start_date, end_date
        
    @staticmethod
    def _parse_date_range(start_date: str | datetime.date, end_date: str | datetime.date, rollback_period: str | Literal['ytd']) -> tuple[datetime.date, datetime.date]:
        from pfeed.utils import rollback_date_range
        if start_date:
            if isinstance(start_date, str):
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                yesterday = datetime.datetime.now(tz=datetime.timezone.utc).date() - datetime.timedelta(days=1)
                end_date = yesterday
        else:
            assert rollback_period != 'max', '"max" is not allowed for `rollback_period`'
            start_date, end_date = rollback_date_range(rollback_period)
        assert start_date <= end_date, f"start_date must be before end_date: {start_date} <= {end_date}"
        return start_date, end_date    
  
    def _create_batch_dataflows(
        self, 
        extract_func: Callable,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        dataflow_per_date: bool, 
        start_date: datetime.date, 
        end_date: datetime.date,
        extract_type: ExtractType,
    ) -> list[DataFlow]:
        from pandas import date_range
        dataflows: list[DataFlow] = []
        def _add_dataflow(data_model_start_date: datetime.date, data_model_end_date: datetime.date):
            dataflow_data_model = partial_dataflow_data_model(start_date=data_model_start_date, end_date=data_model_end_date)
            faucet_data_model = partial_faucet_data_model(start_date=data_model_start_date, end_date=data_model_end_date)
            faucet: Faucet = self._create_faucet(data_model=faucet_data_model, extract_func=extract_func, extract_type=extract_type)
            dataflow: DataFlow = self.create_dataflow(dataflow_data_model, faucet)
            dataflows.append(dataflow)
        
        if dataflow_per_date:
            # one dataflow per date
            for date in date_range(start_date, end_date).date:
                _add_dataflow(date, date)
        else:
            # one dataflow for the entire date range
            _add_dataflow(start_date, end_date)
        return dataflows
    
    @clear_subflows
    def _run_retrieve(
        self,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        start_date: datetime.date,
        end_date: datetime.date,
        data_layer: tDataLayer,
        data_domain: str,
        from_storage: tStorage,
        storage_options: dict | None,
        add_default_transformations: Callable,
        dataflow_per_date: bool,
        include_metadata: bool,
    ) -> GenericFrameOrNone | GenericFrameOrNoneWithMetadata | TimeBasedFeed:
        from pfeed.config import get_config
        config = get_config()
        self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(
                data_path=config.data_path,
                data_model=data_model,
                data_layer=data_layer,
                data_domain=data_domain,
                from_storage=from_storage,
                storage_options=storage_options,
            ),
            partial_dataflow_data_model=partial_dataflow_data_model,
            partial_faucet_data_model=partial_faucet_data_model,
            dataflow_per_date=dataflow_per_date,
            start_date=start_date,
            end_date=end_date,
            extract_type=ExtractType.retrieve,
        )
        add_default_transformations()
        if not self._pipeline_mode:
            return self.run(include_metadata=include_metadata)
        else:
            return self
    
    def _add_default_transformations_to_retrieve(self, *args, **kwargs):
        from pfeed.utils import lambda_with_name
        from pfeed._etl.base import convert_to_desired_df
        self.transform(
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_desired_df(df, self._data_tool)
            )
        )
        
    @clear_subflows
    def _run_download(
        self,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        start_date: datetime.date, 
        end_date: datetime.date,
        dataflow_per_date: bool, 
        include_metadata: bool,
        add_default_transformations: Callable,
        load_to_storage: Callable | None,
    ) -> GenericFrameOrNone | GenericFrameOrNoneWithMetadata | TimeBasedFeed:
        self._create_batch_dataflows(
            extract_func=lambda data_model: self._download_impl(data_model),
            partial_dataflow_data_model=partial_dataflow_data_model,
            partial_faucet_data_model=partial_faucet_data_model,
            dataflow_per_date=dataflow_per_date, 
            start_date=start_date, 
            end_date=end_date,
            extract_type=ExtractType.download,
        )
        add_default_transformations()
        if load_to_storage:
            if not self._pipeline_mode:
                load_to_storage()
            else:
                print_warning('"to_storage" in download() is ignored in pipeline mode, please use .load(to_storage=...) instead, same for "storage_options"')
                
        if not self._pipeline_mode:
            metadata: TimeBasedFeedMetadata
            df, metadata = self.run(include_metadata=True)
            if missing_dates := metadata.get('missing_dates', []):
                self.logger.warning(
                    f'[INCOMPLETE] Download, missing dates:\n'
                    f'{pformat([str(date) for date in missing_dates])}'
                )
            return df if not include_metadata else (df, metadata)
        else:
            return self
    
    @clear_subflows
    def _run_stream(
        self,
        data_model: TimeBasedDataModel,
        add_default_transformations: Callable,
        load_to_storage: Callable | None,
        callback: Callable[[dict], Awaitable[None] | None] | None,
    ) -> None | TimeBasedFeed:
        # reuse existing faucet for streaming dataflows since they share the same extract_func
        if self.streaming_dataflows:
            existing_dataflow = self.streaming_dataflows[0]
            assert existing_dataflow.data_model.env == data_model.env, \
                f'env mismatch in streaming: {existing_dataflow.data_model.env} != {data_model.env}'
            faucet: Faucet = existing_dataflow.faucet
        else:
            faucet: Faucet = self._create_faucet(
                data_model=data_model,
                extract_func=self._stream_impl,
                extract_type=ExtractType.stream,
                close_stream=self._close_stream,
            )
        if callback:
            faucet.set_streaming_callback(callback)
        dataflow: DataFlow = self.create_dataflow(data_model=data_model, faucet=faucet)
        faucet.bind_data_model_to_dataflow(data_model, dataflow)
        add_default_transformations()
        if load_to_storage:
            if not self._pipeline_mode:
                load_to_storage()
            else:
                print_warning('"to_storage" in stream() is ignored in pipeline mode, please use .load(to_storage=...) instead, same for "storage_options"')
        if not self._pipeline_mode:
            return self.run()
        else:
            return self
    
    def _create_streaming_settings(self, mode: StreamMode | str, flush_interval: int):
        self._streaming_settings = StreamingSettings(mode=mode, flush_interval=flush_interval)
  
    def _eager_run_batch(self, ray_kwargs: dict, prefect_kwargs: dict, include_metadata: bool=False) -> GenericFrameOrNone | GenericFrameOrNoneWithMetadata:
        '''Runs dataflows and handles the results.'''
        import narwhals as nw
        
        completed_dataflows, failed_dataflows = self._run_batch_dataflows(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs)

        dfs: list[GenericFrameOrNone] = []
        metadata: TimeBasedFeedMetadata = {'missing_dates': []}
        
        for dataflow in completed_dataflows + failed_dataflows:
            data_model: TimeBasedDataModel = dataflow.data_model
            result: FlowResult = dataflow.result
            _df: GenericFrameOrNone = result.data
            _metadata: dict[str, Any] | StorageMetadata | TimeBasedStorageMetadata = result.metadata

            dfs.append(_df)

            # Handle missing dates
            # NOTE: only data read from storage will have 'missing_dates' in metadata
            # downloaded data will need to create 'missing_dates' by itself by checking if _df is None
            if 'missing_dates' in _metadata:
                metadata['missing_dates'].extend(_metadata['missing_dates'])
            elif _df is None:
                metadata['missing_dates'].extend(data_model.dates)

        dfs: list[Frame] = [nw.from_native(df) for df in dfs if df is not None and not is_empty_dataframe(df)]
        if dfs:
            df: Frame = nw.concat(dfs)
            columns = df.collect_schema().names()
            if 'date' in columns:
                df: Frame = df.sort(by='date', descending=False)
            df: GenericFrame = nw.to_native(df)
        else:
            df = None

        return df if not include_metadata else (df, metadata)
    
    async def _eager_run_stream(self, ray_kwargs: dict):
        return await self._run_stream_dataflows(ray_kwargs=ray_kwargs)
    
    # DEPRECATED: trying to do too much, let users handle it
    # def _get_historical_data_impl(
    #     self,
    #     product: str,
    #     symbol: str,
    #     rollback_period: str | Literal['ytd', 'max'],
    #     start_date: str,
    #     end_date: str,
    #     data_origin: str,
    #     data_layer: tDataLayer | None,
    #     data_domain: str,
    #     from_storage: tStorage | None,
    #     to_storage: tStorage | None,
    #     storage_options: dict | None,
    #     force_download: bool,
    #     retrieve_per_date: bool,
    #     product_specs: dict | None,
    #     **feed_kwargs
    # ) -> GenericFrameOrNone:
    #     '''
    #     """Gets historical data from Yahoo Finance using yfinance's Ticker.history().
    #     Args:
    #         product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP
    #         symbol: symbol that will be used by yfinance's Ticker.history().
    #             If not specified, it will be derived from `product`, which might be inaccurate.
    #         rollback_period: Data resolution or 'ytd' or 'max'
    #             Period to rollback from today, only used when `start_date` is not specified.
    #             Default is '1M' = 1 month.
    #             if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
    #         force_download: Whether to skip retrieving data from storage.
    #         feed_kwargs: kwargs specific to the feed, e.g. "resolution" for MarketFeed
    #     """
    #     '''
    #     import pandas as pd
    #     import narwhals as nw
    #     from pfeed.enums import DataAccessType
        
    #     assert not self._pipeline_mode, 'pipeline mode is not supported in get_historical_data()'
    #     kwargs = product_specs or {}
    #     kwargs.update(feed_kwargs)
        
    #     is_download_required = force_download

    #     if not force_download:
    #         search_data_layers = [DataLayer.CURATED, DataLayer.CLEANED] if data_layer is None else [DataLayer[data_layer.upper()]]
    #         dfs_from_storage: list[Frame] = []
    #         start_missing_date = start_date
    #         end_missing_date = end_date
    #         for search_data_layer in search_data_layers:
    #             df_from_storage, metadata_from_storage = self.retrieve(
    #                 product=product,
    #                 rollback_period=rollback_period,
    #                 start_date=start_missing_date,
    #                 end_date=end_missing_date,
    #                 data_origin=data_origin,
    #                 data_layer=search_data_layer.name,
    #                 data_domain=data_domain,
    #                 dataflow_per_date=retrieve_per_date,
    #                 from_storage=from_storage,
    #                 storage_options=storage_options,
    #                 include_metadata=True,
    #                 env=Environment.BACKTEST,
    #                 **kwargs
    #             )

    #             if missing_dates := metadata_from_storage['missing_dates']:
    #                 is_download_required = True
    #                 # fill gaps between missing dates 
    #                 start_missing_date = str(min(missing_dates))
    #                 end_missing_date = str(max(missing_dates))
    #                 missing_dates = pd.date_range(start_missing_date, end_missing_date).date.tolist()
    #             else:
    #                 is_download_required = False
                
    #             if df_from_storage is not None:
    #                 # REVIEW
    #                 if isinstance(df_from_storage, pd.DataFrame):
    #                     # convert to pyarrow's dtypes to avoid narwhals error: Accessing `date` on the default pandas backend will return a Series of type `object`. 
    #                     # This differs from polars API and will prevent `.dt` chaining. Please switch to the `pyarrow` backend
    #                     df_from_storage = df_from_storage.convert_dtypes(dtype_backend="pyarrow")
                        
    #                 df_from_storage: Frame = nw.from_native(df_from_storage)
    #                 if missing_dates:
    #                     # remove missing dates in df_from_storage to avoid duplicates between data from retrieve() and download() since downloads will include all dates in range
    #                     df_from_storage: Frame = df_from_storage.filter(~nw.col('date').dt.date().is_in(missing_dates))
    #                 dfs_from_storage.append(df_from_storage)

    #             # no missing data, stop searching different data layers
    #             if not missing_dates:
    #                 break

    #         dfs: list[Frame] = [df for df in dfs_from_storage if not is_empty_dataframe(df)] if dfs_from_storage else []
    #         df_from_storage: Frame | None = nw.concat(dfs) if dfs else None
    #     else:
    #         df_from_storage: Frame | None = None
    #         start_missing_date = start_date
    #         end_missing_date = end_date
        
    #     if is_download_required:
    #         # REVIEW: check if the condition here is correct, can't afford casually downloading paid data and incur charges
    #         if self.data_source.access_type == DataAccessType.PAID_BY_USAGE and \
    #             (to_storage is None or DataStorage[to_storage.upper()] == DataStorage.CACHE):
    #             raise Exception(f'downloading PAID data to {to_storage=} is not allowed')
                
    #         df_from_source = self.download(
    #             product,
    #             symbol=symbol,
    #             rollback_period=rollback_period,
    #             start_date=start_missing_date,
    #             end_date=end_missing_date,
    #             data_origin=data_origin,
    #             to_storage=to_storage,
    #             storage_options=storage_options,
    #             **kwargs
    #         )
    #         if df_from_source is not None:
    #             df_from_source: Frame = nw.from_native(df_from_source)
    #     else:
    #         df_from_source: Frame | None = None
            
    #     if df_from_storage is not None and df_from_source is not None:
    #         df: Frame = nw.concat([df_from_storage, df_from_source])
    #     elif df_from_storage is not None:
    #         df: Frame = df_from_storage
    #     elif df_from_source is not None:
    #         df: Frame = df_from_source
    #     else:
    #         df = None

    #     if df is not None:
    #         df: Frame = df.sort(by='date', descending=False)
    #         df: GenericFrame = df.to_native()
            
    #         # REVIEW
    #         if isinstance(df, pd.DataFrame):
    #             # convert pyarrow's "timestamp[ns]" back to pandas' "datetime64[ns]" for consistency
    #             df['date'] = df['date'].astype('datetime64[ns]')
            
    #     return df
    