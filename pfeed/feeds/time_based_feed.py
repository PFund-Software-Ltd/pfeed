# pyright: reportCallIssue=false, reportUnknownMemberType=false, reportAttributeAccessIssue=false
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, ClassVar, Callable, Any, cast
if TYPE_CHECKING:
    from narwhals.typing import Frame, IntoFrame
    from pfund.datas.resolution import Resolution
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.result import DataFlowResult, RunResult
    from pfeed.dataflow.faucet import Faucet
    from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

import datetime
from abc import ABC

import polars as pl

from pfund_kit.style import TextStyle, RichColor
from pfeed.feeds.base_feed import BaseFeed


__all__ = []


class TimeBasedFeed(BaseFeed, ABC):
    data_model_class: ClassVar[type[TimeBasedDataModel]]
    date_columns_in_raw_data: ClassVar[list[str]]
    DATE_COL_IN_CLEANED_DATA: ClassVar[str] = 'date'
    DATE_COL_IN_RAW_DATA: ClassVar[str] = '_pfeed_date'

    @classmethod
    def _standardize_date_column(cls, df: pl.LazyFrame, is_raw_data: bool) -> pl.LazyFrame:
        '''Materialize a uniform date column for downstream filtering and dedup.

        Sources expose their date under different column names (e.g. Bybit: 'timestamp',
        Yahoo Finance: 'Datetime'/'Date'). `date_columns_in_raw_data` lists the candidates
        to look for in the input. Handling differs by data layer so raw data stays a
        faithful mirror of the source:
            - Cleaned: the source's date column is renamed to 'date'.
            - Raw: the source schema is left untouched and a '_pfeed_date' column is
            added as a copy, used by the storage handler for date-based filtering
            and dedup.

        Args:
            df: Input LazyFrame containing one of the source's date columns listed in
                `date_columns_in_raw_data`.
            is_raw_data: If True, preserve the source schema and add '_pfeed_date'.
                If False, rename the source's date column to 'date'.

        Returns:
            A LazyFrame with a normalized date column ('date' for cleaned data,
            '_pfeed_date' for raw data).

        Raises:
            ValueError: If none of the candidate source date columns are present in `df`.
        '''
        from pfeed._etl.base import standardize_date_column

        cols = df.collect_schema().names()
        raw_date_col = next(
            (c for c in cls.date_columns_in_raw_data if c in cols),
            None,
        )
        if raw_date_col is None:
            raise ValueError(
                f"no date column ({cls.date_columns_in_raw_data}) found in {cols}"
            )

        if not is_raw_data:
            date_col = cls.DATE_COL_IN_CLEANED_DATA
            df = df.rename({raw_date_col: date_col})
        else:
            date_col = cls.DATE_COL_IN_RAW_DATA
            df = df.with_columns(pl.col(raw_date_col).alias(date_col))
        return standardize_date_column(df, date_col)

    def _rollback_max_period(self, _: Resolution) -> tuple[datetime.date | str | None, datetime.date | str | None, str]:
        data_source_start_date = cast(datetime.date, self.data_source.METADATA.start_date)
        if data_source_start_date:
            start_date = data_source_start_date
            end_date = None
            rollback_period = 'max'
        else:
            raise ValueError(f'{self.name} has no data source start_date, cannot use rollback_period="max"')
        return start_date, end_date, rollback_period

    def _standardize_dates(
        self,
        resolution: Resolution,
        start_date: str | datetime.date | None,
        end_date: str | datetime.date | None,
        rollback_period: Resolution | str | Literal['ytd', 'max'],
    ) -> tuple[datetime.date, datetime.date]:
        '''Standardize start_date and end_date based on input parameters.

        Args:
            resolution: The resolution of the data, only used when rollback_period is 'max'.
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
        from pfeed.utils.temporal import parse_date_range
        if rollback_period.lower() == 'max' and not start_date:
            if not self.SUPPORTS_ROLLBACK_MAX_PERIOD:
                raise ValueError(f"rollback_period='max' is not supported by {self.name}")
            start_date, end_date, rollback_period = self._rollback_max_period(resolution)
        start_date, end_date = parse_date_range(start_date, end_date, rollback_period)
        return start_date, end_date

    def _create_batch_dataflows(self, extract_func: Callable[[TimeBasedDataModel], Any]) -> list[DataFlow]:
        request = cast("TimeBasedFeedBaseRequest", self._get_current_request())
        self.logger.debug(
            f'{request.name}:\n{request}\n',
            style=TextStyle.BOLD + RichColor.GREEN
        )
        data_model = cast("TimeBasedDataModel", self._create_data_model_from_request(request))
        faucet: Faucet = self._create_faucet(data_source=data_model.data_source, extract_func=extract_func, extract_type=request.extract_type)
        if request.dataflow_per_date:
            data_models = [
                data_model.model_copy(update={'start_date': d, 'end_date': d})
                for d in data_model.dates
            ]
        else:
            data_models = [data_model]
        dataflows = [self._create_dataflow(faucet=faucet, data_model=dm) for dm in data_models]
        self._dataflows[request] = dataflows
        return dataflows

    def run(self, **prefect_kwargs: Any) -> RunResult:
        '''Runs dataflows and returns a RunResult exposing the combined frame,
        per-flow successes/failures, and any dates that produced no data.'''
        import narwhals as nw
        from pfeed.utils.dataframe import is_empty_dataframe
        from pfeed.dataflow.result import RunResult

        result_dfs: list[IntoFrame] = []

        dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)
        for dataflow in dataflows:
            result: DataFlowResult = dataflow.result
            _df: IntoFrame | None = result.data
            if _df is not None:
                result_dfs.append(_df)

        dfs: list[Frame] = [nw.from_native(df) for df in result_dfs if not is_empty_dataframe(df)]
        if dfs:
            from pfeed._etl.base import convert_dataframe
            df: Frame = cast("Frame", nw.concat(dfs))  # pyright: ignore[reportArgumentType]
            schema = df.collect_schema()
            columns = schema.names()
            if 'date' in columns and schema['date'].is_temporal():
                df: Frame = df.sort(by='date', descending=False)
            # Storage-backed flows return raw pl.LazyFrame from storage.read,
            # bypassing the per-flow `convert_to_user_df` transformation. Convert
            # once here so the aggregated frame matches the user's data_tool.
            combined: IntoFrame | None = convert_dataframe(nw.to_native(df))
        else:
            combined = None

        return RunResult(data=combined, dataflows=dataflows)
