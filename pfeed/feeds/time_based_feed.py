from __future__ import annotations
from typing import TYPE_CHECKING, Literal, ClassVar, Callable, Any
if TYPE_CHECKING:
    import pandas as pd
    from narwhals.typing import Frame
    from pfeed.typing import GenericFrame
    from pfeed.dataflow.dataflow import DataFlow, DataFlowResult
    from pfeed.dataflow.faucet import Faucet
    from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest

import datetime

from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


__all__ = ["TimeBasedFeed"]


class TimeBasedFeed(BaseFeed):
    data_model_class: ClassVar[type[TimeBasedDataModel]]
    date_column_in_raw_data: ClassVar[str]
    original_date_column_in_raw_data: ClassVar[str] = 'original_date'

    def _standardize_date_column(self, df: pd.DataFrame, is_raw_data: bool) -> pd.DataFrame:
        '''Ensure date column is standardized, "date" column is mandatory for storing data'''
        from pfeed._etl.base import standardize_date_column
        if not is_raw_data:
            df = df.rename(columns={self.date_column_in_raw_data: 'date'})
        else:
            if self.date_column_in_raw_data != 'date':
                df['date'] = df[self.date_column_in_raw_data]
            else:
                # NOTE: if raw data also uses "date" column, copy it to "original_date" column, 
                # which will be renamed back to "date" before writing to storage
                df[self.original_date_column_in_raw_data] = df['date']
        df = standardize_date_column(df)
        return df
    
    def _standardize_dates(
        self, 
        start_date: str | datetime.date, 
        end_date: str | datetime.date,
        rollback_period: str | Literal['ytd', 'max'],
    ) -> tuple[datetime.date, datetime.date]:
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
        from pfeed.utils import parse_date_range
        if rollback_period == 'max' and not start_date:
            if hasattr(self.data_source, 'start_date') and self.data_source.start_date:
                start_date = self.data_source.start_date
            else:
                raise ValueError(f'{self.name} {rollback_period=} is not supported')        
        start_date, end_date = parse_date_range(start_date, end_date, rollback_period)
        return start_date, end_date
  
    def _create_batch_dataflows(self, extract_func: Callable):
        self._clear_dataflows()
        request: TimeBasedFeedBaseRequest = self._current_request
        data_model: TimeBasedDataModel = self._create_data_model_from_request(request)
        if request.dataflow_per_date:
            # one dataflow per date
            for date in data_model.dates:
                # NOTE: update data_model to a single date since it is one dataflow per date
                data_model_copy = data_model.model_copy(deep=False)
                data_model_copy.update_start_date(date)
                data_model_copy.update_end_date(date)
                faucet: Faucet = self._create_faucet(data_model=data_model_copy, extract_func=extract_func, extract_type=request.extract_type)
                dataflow: DataFlow = self._create_dataflow(data_model_copy, faucet)
                self._dataflows.append(dataflow)
        else:
            # one dataflow for the entire date range
            faucet: Faucet = self._create_faucet(data_model=data_model, extract_func=extract_func, extract_type=request.extract_type)
            dataflow: DataFlow = self._create_dataflow(data_model, faucet)
            self._dataflows.append(dataflow)
    
    def run(self, **prefect_kwargs: Any) -> GenericFrame | None:
        '''Runs dataflows and handles the results.'''
        import narwhals as nw
        from pfeed.utils.dataframe import is_empty_dataframe
        
        completed_dataflows, failed_dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)

        dfs: list[GenericFrame] = []
        
        for dataflow in completed_dataflows + failed_dataflows:
            result: DataFlowResult = dataflow.result
            _df: GenericFrame | None = result.data
            if _df is not None:
                dfs.append(_df)

        dfs: list[Frame] = [nw.from_native(df) for df in dfs if not is_empty_dataframe(df)]
        if dfs:
            df: Frame = nw.concat(dfs)
            schema = df.collect_schema()
            columns = schema.names()
            if 'date' in columns:
                date_dtype = schema['date']
                # Check if 'date' column is a temporal type (Date or Datetime) before sorting
                if 'Date' in str(date_dtype) or 'Datetime' in str(date_dtype):
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
    