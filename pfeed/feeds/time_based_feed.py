from __future__ import annotations
from typing import TYPE_CHECKING, TypedDict, Literal, Callable, Any, ClassVar
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfeed._io.base_io import StorageMetadata
    from pfeed.data_handlers.time_based_data_handler import TimeBasedStorageMetadata
    from pfeed.typing import GenericFrame
    from pfeed.dataflow.dataflow import DataFlow, FlowResult
    from pfeed.dataflow.faucet import Faucet
    from pfeed.enums import DataStorage, DataLayer
    from pfeed.requests.time_based_feed_download_request import TimeBasedFeedDownloadRequest
    class TimeBasedFeedMetadata(TypedDict, total=True):
        missing_dates: list[datetime.date]

import datetime
from abc import abstractmethod

from pfeed.enums import ExtractType
from pfeed.config import get_config
from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


config = get_config()


__all__ = ["TimeBasedFeed"]


class TimeBasedFeed(BaseFeed):
    data_model_class: ClassVar[type[TimeBasedDataModel]]

    @abstractmethod
    def _standardize_date_column(data: Any) -> Any:
        pass
    
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
            if hasattr(self.data_source, 'start_date') and self.data_source.start_date:
                start_date = self.data_source.start_date
            else:
                raise ValueError(f'{self.name} {rollback_period=} is not supported')        
        start_date, end_date = self._parse_date_range(start_date, end_date, rollback_period)
        return start_date, end_date
        
    # FIXME: move to utils
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
  
    def _create_batch_dataflows(self, extract_func: Callable, extract_type: ExtractType) -> list[DataFlow]:
        self._clear_dataflows()
        request: TimeBasedFeedDownloadRequest = self._current_request
        dataflows: list[DataFlow] = []
        data_model: TimeBasedDataModel = self._create_data_model_from_request(request)
        if request.dataflow_per_date:
            # one dataflow per date
            for date in data_model.dates:
                # NOTE: update data_model to a single date since it is one dataflow per date
                data_model_copy = data_model.model_copy(deep=False)
                data_model_copy.update_start_date(date)
                data_model_copy.update_end_date(date)
                faucet: Faucet = self._create_faucet(data_model=data_model_copy, extract_func=extract_func, extract_type=extract_type)
                dataflow: DataFlow = self._create_dataflow(data_model_copy, faucet)
                dataflows.append(dataflow)
        else:
            # one dataflow for the entire date range
            faucet: Faucet = self._create_faucet(data_model=data_model, extract_func=extract_func, extract_type=extract_type)
            dataflow: DataFlow = self._create_dataflow(data_model, faucet)
            dataflows.append(dataflow)
        return dataflows
    
    def _run_retrieve(
        self,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        start_date: datetime.date,
        end_date: datetime.date,
        data_layer: DataLayer,
        from_storage: DataStorage,
        storage_options: dict | None,
        add_default_transformations: Callable,
        dataflow_per_date: bool,
    ) -> GenericFrame | None | TimeBasedFeed:
        from pfeed.config import get_config
        config = get_config()
        self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(
                data_path=config.data_path,
                data_model=data_model,
                data_layer=data_layer,
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
            return self.run()
        else:
            return self
    
    def _add_default_transformations_to_retrieve(self, *args, **kwargs):
        from pfeed.utils import lambda_with_name
        from pfeed._etl.base import convert_to_desired_df
        self.transform(
            lambda_with_name(
                '__convert_to_user_df',
                lambda df: convert_to_desired_df(df, config.data_tool)
            )
        )
    
    def run(self, prefect_kwargs: dict | None=None) -> GenericFrame | None:
        '''Runs dataflows and handles the results.'''
        import narwhals as nw
        from pfeed.utils.dataframe import is_empty_dataframe
        
        completed_dataflows, failed_dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)

        dfs: list[GenericFrame | None] = []
        metadata: TimeBasedFeedMetadata = {'missing_dates': []}
        
        for dataflow in completed_dataflows + failed_dataflows:
            data_model: TimeBasedDataModel = dataflow.data_model
            result: FlowResult = dataflow.result
            _df: GenericFrame | None = result.data
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

        return df
    
    # DEPRECATED: trying to do too much, let users handle it
    # def _get_historical_data_impl(
    #     self,
    #     product: str,
    #     symbol: str,
    #     rollback_period: str | Literal['ytd', 'max'],
    #     start_date: str,
    #     end_date: str,
    #     data_origin: str,
    #     data_layer: DataLayer | None,
    #     data_domain: str,
    #     from_storage: DataStorage | None,
    #     to_storage: DataStorage | None,
    #     storage_options: dict | None,
    #     force_download: bool,
    #     retrieve_per_date: bool,
    #     product_specs: dict | None,
    #     **feed_kwargs
    # ) -> GenericFrame | None:
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
    