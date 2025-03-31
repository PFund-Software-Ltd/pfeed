from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Any, Callable
if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import Frame
    from pfeed.typing import GenericFrame
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel
    from pfeed.flows.dataflow import DataFlow, FlowResult
    from pfeed.flows.faucet import Faucet
    from pfeed.typing import tSTORAGE, tDATA_LAYER

import datetime
from pprint import pformat

from pfeed.utils.dataframe import is_empty_dataframe
from pfeed.enums import ExtractType, DataLayer
from pfeed.feeds.base_feed import BaseFeed, clear_subflows


__all__ = ["TimeBasedFeed"]


class TimeBasedFeed(BaseFeed):
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
        from pfeed.utils.utils import rollback_date_range
        if start_date or rollback_period == 'max':
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            else:
                if self.data_source.start_date:
                    start_date = self.data_source.start_date
                else:
                    raise ValueError(f'{self.name} {rollback_period=} is not supported')
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                yesterday = datetime.datetime.now(tz=datetime.timezone.utc).date() - datetime.timedelta(days=1)
                end_date = yesterday
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        return start_date, end_date
  
    @clear_subflows
    def _create_dataflows(
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
            faucet: Faucet = self._create_faucet(faucet_data_model, extract_func, extract_type)
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
    
    def _run_retrieve(
        self,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        start_date: datetime.date,
        end_date: datetime.date,
        data_layer: tDATA_LAYER | None,
        data_domain: str,
        from_storage: tSTORAGE | None,
        storage_options: dict | None,
        add_default_transformations: Callable | None,
        dataflow_per_date: bool,
        include_metadata: bool,
    ) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]] | TimeBasedFeed:
        if dataflow_per_date and data_layer.upper() == DataLayer.CURATED:
            self.logger.info(f'{dataflow_per_date=} is not supported when data_layer is "curated" (files are NOT per date), set dataflow_per_date to False')
            dataflow_per_date = False
        self._create_dataflows(
            lambda _data_model: self._retrieve_impl(
                data_model=_data_model,
                data_layer=data_layer,
                data_domain=data_domain,
                from_storage=from_storage,
                storage_options=storage_options,
            ),
            partial_dataflow_data_model,
            partial_faucet_data_model,
            dataflow_per_date,
            start_date,
            end_date,
            extract_type=ExtractType.retrieve,
        )
        if add_default_transformations:
            add_default_transformations()
        if not self._pipeline_mode:
            return self._eager_run(include_metadata=include_metadata)
        else:
            return self
        
    def _run_download(
        self,
        partial_dataflow_data_model: Callable,
        partial_faucet_data_model: Callable,
        start_date: datetime.date, 
        end_date: datetime.date,
        data_layer: Literal['raw', 'cleaned'],
        data_domain: str,
        to_storage: tSTORAGE | None,
        storage_options: dict | None,
        dataflow_per_date: bool, 
        include_metadata: bool,
        add_default_transformations: Callable | None,
    ) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]] | TimeBasedFeed:
        assert data_layer.upper() != DataLayer.CURATED, 'writing to "curated" data layer is not supported in download()'
        self._create_dataflows(
            lambda _data_model: self._download_impl(_data_model),
            partial_dataflow_data_model,
            partial_faucet_data_model,
            dataflow_per_date, 
            start_date, 
            end_date,
            extract_type=ExtractType.download,
        )
        if add_default_transformations:
            add_default_transformations()
        if not self._pipeline_mode:
            if to_storage is not None:
                self.load(
                    to_storage=to_storage, 
                    data_layer=data_layer, 
                    data_domain=data_domain, 
                    storage_options=storage_options,
                )
            df, metadata = self._eager_run(include_metadata=True)
            if missing_dates := metadata.get('missing_dates', []):
                self.logger.warning(
                    f'[INCOMPLETE] Download, missing dates:\n'
                    f'{pformat([str(date) for date in missing_dates])}'
                ) 
            return df if not include_metadata else (df, metadata)
        else:
            return self
    
    def _retrieve_impl(
        self,
        data_model: TimeBasedDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None,
        storage_options: dict | None,
    ) -> tuple[GenericFrame | None, dict[str, Any]]:
        '''Retrieves data among all scanned data loaded from local storages.
        Returns the data from the storage with the least missing dates.
        '''
        from pfeed._etl.base import convert_to_user_df
        df_in_storages, metadata_in_storages = super()._retrieve_impl(
            data_model=data_model,
            data_domain=data_domain,
            data_layer=data_layer,
            from_storage=from_storage,
            storage_options=storage_options,
        )

        # choose the storage with the least missing dates
        missing_dates_in_storages: dict[tSTORAGE, list[datetime.date]] = {
            storage: metadata['missing_dates']
            for storage, metadata in metadata_in_storages.items()
        }
        storage_with_the_least_missing_dates = min(missing_dates_in_storages, key=lambda x: len(missing_dates_in_storages[x]))
        
        polars_lf: pl.LazyFrame | None = df_in_storages[storage_with_the_least_missing_dates]
        metadata = metadata_in_storages[storage_with_the_least_missing_dates]
        if polars_lf is not None:
            df: GenericFrame = convert_to_user_df(polars_lf, self.data_tool.name)
            self.logger.info(f'found data {data_model} in {storage_with_the_least_missing_dates} ({data_layer=})')
        else:
            df = None
            self.logger.debug(f'failed to find data {data_model} in any storage ({data_layer=})')
        return df, metadata
    
    def _get_historical_data_impl(
        self,
        product: str,
        symbol: str,
        rollback_period: str | Literal['ytd', 'max'],
        start_date: str,
        end_date: str,
        data_origin: str,
        data_layer: tDATA_LAYER | None,
        data_domain: str,
        from_storage: tSTORAGE | None,
        to_storage: tSTORAGE | None,
        storage_options: dict | None,
        force_download: bool,
        product_specs: dict | None,
        **feed_kwargs
    ) -> GenericFrame | None:
        '''
        """Gets historical data from Yahoo Finance using yfinance's Ticker.history().
        Args:
            product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP
            symbol: symbol that will be used by yfinance's Ticker.history().
                If not specified, it will be derived from `product`, which might be inaccurate.
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
                if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
            force_download: Whether to skip retrieving data from storage.
            feed_kwargs: kwargs specific to the feed, e.g. "resolution" for MarketFeed
        """
        '''
        import pandas as pd
        import narwhals as nw
        from pfeed.enums import DataAccessType
        
        assert not self._pipeline_mode, 'pipeline mode is not supported in get_historical_data()'
        kwargs = product_specs or {}
        kwargs.update(feed_kwargs)
        
        is_download_required = force_download

        if not force_download:
            search_data_layers = [DataLayer.CURATED, DataLayer.CLEANED] if data_layer is None else [DataLayer[data_layer.upper()]]
            dfs_from_storage: list[Frame] = []
            start_missing_date = start_date
            end_missing_date = end_date
            for search_data_layer in search_data_layers:
                df_from_storage, metadata_from_storage = self.retrieve(
                    product=product,
                    rollback_period=rollback_period,
                    start_date=start_missing_date,
                    end_date=end_missing_date,
                    data_origin=data_origin,
                    data_layer=search_data_layer.name,
                    data_domain=data_domain,
                    from_storage=from_storage,
                    storage_options=storage_options,
                    include_metadata=True,
                    **kwargs
                )

                if missing_dates := metadata_from_storage['missing_dates']:
                    is_download_required = True
                    # fill gaps between missing dates 
                    start_missing_date = str(min(missing_dates))
                    end_missing_date = str(max(missing_dates))
                    missing_dates = pd.date_range(start_missing_date, end_missing_date).date.tolist()
                else:
                    is_download_required = False
                
                if df_from_storage is not None:
                    if isinstance(df_from_storage, pd.DataFrame):
                        # convert to pyarrow's dtypes to avoid narwhals error: Accessing `date` on the default pandas backend will return a Series of type `object`. 
                        # This differs from polars API and will prevent `.dt` chaining. Please switch to the `pyarrow` backend
                        df_from_storage = df_from_storage.convert_dtypes(dtype_backend="pyarrow")
                    df_from_storage: Frame = nw.from_native(df_from_storage)
                    if missing_dates:
                        # remove missing dates in df_from_storage to avoid duplicates between data from retrieve() and download() since downloads will include all dates in range
                        df_from_storage: Frame = df_from_storage.filter(~nw.col('date').dt.date().is_in(missing_dates))
                    dfs_from_storage.append(df_from_storage)

                # no missing data, stop searching different data layers
                if not missing_dates:
                    break

            df_from_storage: Frame | None = nw.concat([df for df in dfs_from_storage if not is_empty_dataframe(df)]) if dfs_from_storage else None
        else:
            df_from_storage: Frame | None = None
            start_missing_date = start_date
            end_missing_date = end_date
        
        if is_download_required:
            # REVIEW: check if the condition here is correct, can't afford casually downloading paid data and incur charges
            if self.data_source.access_type == DataAccessType.PAID_BY_USAGE:
                assert to_storage is not None and to_storage.lower() != 'cache', f'for downloading PAID data, `to_storage` cannot be {to_storage}'
                
            df_from_source = self.download(
                product,
                symbol=symbol,
                rollback_period=rollback_period,
                start_date=start_missing_date,
                end_date=end_missing_date,
                data_origin=data_origin,
                data_domain=data_domain,
                to_storage=None,  # to_storage=None means NOT write to storage
                storage_options=storage_options,
                **kwargs
            )
            if df_from_source is not None:
                df_from_source: Frame = nw.from_native(df_from_source)
        else:
            df_from_source: Frame | None = None
            
        if df_from_storage is not None and df_from_source is not None:
            df: Frame = nw.concat([df_from_storage, df_from_source])
        elif df_from_storage is not None:
            df: Frame = df_from_storage
        elif df_from_source is not None:
            df: Frame = df_from_source
        else:
            df = None

        if df is not None:
            df: Frame = df.sort(by='date', descending=False)
            df: GenericFrame = df.to_native()
            
            if isinstance(df, pd.DataFrame):
                # convert pyarrow's "timestamp[ns]" back to pandas' "datetime64[ns]" for consistency
                df['date'] = df['date'].astype('datetime64[ns]')
            
        return df
  
    def _eager_run(self, include_metadata: bool=False) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]]:
        '''Runs dataflows and handles the results.'''
        import narwhals as nw
        
        assert not self._pipeline_mode, 'eager_run() is not supported in pipeline mode'
        
        completed_dataflows, failed_dataflows = self.run()

        dfs: list[GenericFrame | None] = []
        metadata = {'missing_dates': []}
        
        for dataflow in completed_dataflows + failed_dataflows:
            data_model: TimeBasedDataModel = dataflow.data_model
            result: FlowResult = dataflow.result
            _df: GenericFrame | None = result.data
            _metadata: dict[str, Any] = result.metadata
            
            dfs.append(_df)
            
            # Handle missing dates
            # NOTE: only data read from storage will have 'missing_dates' in metadata
            # downloaded data will need to create 'missing_dates' by itself by checking if _df is None
            if 'missing_dates' in _metadata:
                metadata['missing_dates'].extend(_metadata['missing_dates'])
            elif _df is None:
                metadata['missing_dates'].extend(data_model.dates)

        dfs: list[Frame] = [nw.from_native(df) for df in dfs if df is not None]
        if dfs:
            df: Frame = nw.concat([df for df in dfs if not is_empty_dataframe(df)])
            df: GenericFrame = nw.to_native(df)
        else:
            df = None

        return df if not include_metadata else (df, metadata)
