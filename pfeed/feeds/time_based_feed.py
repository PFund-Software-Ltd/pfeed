from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Any
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfeed.typing.core import tDataFrame
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

import datetime

import narwhals as nw

from pfeed.feeds.base_feed import BaseFeed


__all__ = ["TimeBasedFeed"]


class TimeBasedFeed(BaseFeed):
    def _standardize_dates(self, start_date: str, end_date: str, rollback_period: str | Literal['ytd', 'max']) -> tuple[datetime.date, datetime.date]:
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
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            else:
                if self.data_source.start_date:
                    start_date = self.data_source.start_date
                else:
                    raise ValueError(f'{self.name} {rollback_period=} is not supported')
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                yesterday = datetime.datetime.now(tz=datetime.timezone.utc).date() - datetime.timedelta(days=1)
                end_date = yesterday
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        return start_date, end_date
  
    def _eager_run(self, include_metadata: bool=False) -> tDataFrame | None | tuple[tDataFrame | None, dict[str, Any]]:
        '''Runs dataflows and handles the results.'''
        assert not self._pipeline_mode, 'eager_run() is not supported in pipeline mode'
        completed_dataflows, failed_dataflows = self.run()
        dfs: dict[datetime.date, tDataFrame | None] = {}
        metadata = {'missing_dates': []}
        for dataflow in completed_dataflows + failed_dataflows:
            data_model: TimeBasedDataModel = dataflow.data_model
            result = dataflow.result
            df: tDataFrame | None = result.data
            # e.g. retrieve(), yahoo_finance.market_feed.download() all use one dataflow for the entire date range
            if data_model.is_date_range():
                assert len(completed_dataflows) + len(failed_dataflows) == 1, 'only one date-range dataflow is supported'
                missing_file_paths: list[str] = result.metadata.get('missing_file_paths', [])
                missing_dates = [fp.split('/')[-1].rsplit('_', 1)[-1].removesuffix(data_model.file_extension) for fp in missing_file_paths]
                metadata['missing_dates'] = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
                break
            else:
                dfs[data_model.date] = df
        # Case: Multi-Dataflows (dataflow_per_date = True)
        else:
            metadata['missing_dates'] = [date for date, df in dfs.items() if df is None]
            if dfs := [nw.from_native(df) for df in dfs.values() if df is not None]:
                df: Frame = nw.concat(dfs)
                df: tDataFrame = nw.to_native(df)
            else:
                df = None

        return df if not include_metadata else (df, metadata)