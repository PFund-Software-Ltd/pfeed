from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Any, Callable
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfeed.typing import GenericFrame
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel
    from pfeed.flows.dataflow import DataFlow, FlowResult
    from pfeed.typing import tSTORAGE, tDATA_LAYER

import datetime
from pprint import pformat

import pandas as pd
import narwhals as nw

from pfeed.enums import DataStorage
from pfeed.enums.extract_type import ExtractType
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
        partial_data_model: Callable,
        dataflow_per_date: bool, 
        start_date: datetime.date, 
        end_date: datetime.date,
        extract_type: ExtractType,
    ) -> list[DataFlow]:
        dataflows: list[DataFlow] = []
        def _add_dataflow(data_model_start_date: datetime.date, data_model_end_date: datetime.date):
            data_model = partial_data_model(start_date=data_model_start_date, end_date=data_model_end_date)
            faucet = self.create_faucet(data_model, extract_func, extract_type)
            dataflow: DataFlow = self.create_dataflow(faucet)
            dataflows.append(dataflow)
        
        if dataflow_per_date:
            # one dataflow per date
            for date in pd.date_range(start_date, end_date).date:
                _add_dataflow(date, date)
        else:
            # one dataflow for the entire date range
            _add_dataflow(start_date, end_date)
        return dataflows
    
    def _run_retrieve(
        self,
        partial_data_model: Callable,
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
        self._create_dataflows(
            lambda _data_model: self._retrieve_impl(
                data_model=_data_model,
                data_layer=data_layer,
                data_domain=data_domain,
                from_storage=from_storage,
                storage_options=storage_options,
            ),
            partial_data_model,
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
        partial_data_model: Callable,
        start_date: datetime.date, 
        end_date: datetime.date,
        data_layer: tDATA_LAYER,
        data_domain: str,
        to_storage: tSTORAGE,
        storage_options: dict | None,
        dataflow_per_date: bool, 
        include_metadata: bool,
        add_default_transformations: Callable | None,
    ) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]] | TimeBasedFeed:
        self._create_dataflows(
            lambda _data_model: self._download_impl(_data_model),
            partial_data_model, 
            dataflow_per_date, 
            start_date, 
            end_date,
            extract_type=ExtractType.download,
        )
        if add_default_transformations:
            add_default_transformations()
        if not self._pipeline_mode:
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
        to_storage: tSTORAGE,
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
            data_layer:
                'cleaned' (least raw): normalize data (refer to 'normalized' below), also remove all non-standard columns
                    e.g. standard columns in stock data are ts, product, open, high, low, close, volume, dividends, splits
                'normalized' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'date'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'original' (most raw): keep the original data from yfinance, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            force_download: Whether to skip retrieving data from storage.
            feed_kwargs: kwargs specific to the feed, e.g. "resolution" for MarketFeed
        """
        '''
        from pfeed.enums import DataAccessType
        
        assert not self._pipeline_mode, 'pipeline mode is not supported in get_historical_data()'
        kwargs = product_specs or {}
        kwargs.update(feed_kwargs)
        
        is_download_required = force_download
        df_from_storage: GenericFrame | None = None
        df_from_source: GenericFrame | None = None
        missing_dates: list[datetime.date] = []

        if not force_download:
            df_from_storage, metadata = self.retrieve(
                product=product,
                rollback_period=rollback_period,
                start_date=start_date,
                end_date=end_date,
                data_origin=data_origin,
                data_layer=data_layer,
                data_domain=data_domain,
                from_storage=from_storage,
                storage_options=storage_options,
                include_metadata=True,
                **kwargs
            )

            if missing_dates := metadata['missing_dates']:
                is_download_required = True
                # fill gaps between missing dates 
                start_missing_date = str(min(missing_dates))
                end_missing_date = str(max(missing_dates))
                missing_dates = pd.date_range(start_missing_date, end_missing_date).date.tolist()
            
            if df_from_storage is not None:
                if isinstance(df_from_storage, pd.DataFrame):
                    # convert to pyarrow's dtypes to avoid narwhals error: Accessing `date` on the default pandas backend will return a Series of type `object`. 
                    # This differs from polars API and will prevent `.dt` chaining. Please switch to the `pyarrow` backend
                    df_from_storage = df_from_storage.convert_dtypes(dtype_backend="pyarrow")
                df_from_storage: Frame = nw.from_native(df_from_storage)
                if missing_dates:
                    # remove missing dates in df_from_storage to avoid duplicates between data from retrieve() and download() since downloads will include all dates in range
                    df_from_storage: Frame = df_from_storage.filter(~nw.col('date').dt.date().is_in(missing_dates))
        else:
            start_missing_date = start_date
            end_missing_date = end_date
        
        if is_download_required:
            # REVIEW: check if the condition here is correct, can't afford casually downloading paid data and incur charges
            if self.data_source.access_type == DataAccessType.PAID_BY_USAGE:
                if to_storage.lower() == 'cache':
                    self.logger.warning('prevent downloading paid data to cache, set `to_storage` to "local"')
                    to_storage = 'local'
            df_from_source = self.download(
                product,
                symbol=symbol,
                rollback_period=rollback_period,
                start_date=start_missing_date,
                end_date=end_missing_date,
                data_origin=data_origin,
                data_layer='curated',
                data_domain=data_domain,
                to_storage=to_storage,
                storage_options=storage_options,
                **kwargs
            )
            if df_from_source is not None:
                df_from_source: Frame = nw.from_native(df_from_source)
            
        df_from_storage: Frame | None
        df_from_source: Frame | None
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
        return df
  
    def _eager_run(self, include_metadata: bool=False) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]]:
        '''Runs dataflows and handles the results.'''
        assert not self._pipeline_mode, 'eager_run() is not supported in pipeline mode'
        
        completed_dataflows, failed_dataflows = self.run()

        dfs: list[GenericFrame | None] = []
        metadata = {'missing_dates': []}
        
        for dataflow in completed_dataflows + failed_dataflows:
            data_model: TimeBasedDataModel = dataflow.data_model
            result: FlowResult = dataflow.result
            _df: GenericFrame | None = result.data
            
            # REVIEW: currently metadata's handling is messy, consider split FlowResult into RetrieveResult and DownloadResult etc.
            # and define their own returned metadata
            
            # Handle missing dates
            if 'missing_dates' in result.metadata:
                metadata['missing_dates'].extend(result.metadata['missing_dates'])
            elif _df is None:
                metadata['missing_dates'].extend(data_model.dates)
            else:
                dfs.append(_df)

        dfs: list[Frame] = [nw.from_native(df) for df in dfs]
        if dfs:
            df: Frame = nw.concat(dfs)
            df: GenericFrame = nw.to_native(df)
        else:
            df = None

        return df if not include_metadata else (df, metadata)