from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from narwhals.typing import Frame, IntoFrame
    from pfund.datas.resolution import Resolution

    from pfeed.base.time_based_data_model import TimeBasedDataModel
    from pfeed.base.time_based_request import TimeBasedFeedBaseRequest
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.result import DataFlowResult, RunResult
    from pfeed.source import BaseSource

import datetime
from abc import ABC

import polars as pl
from pfund_kit.style import RichColor, TextStyle

from pfeed.base.feed import BaseFeed
from pfeed.utils.temporal import parse_date_range


class TimeBasedFeed[
    SourceT: BaseSource,
    RequestT: TimeBasedFeedBaseRequest,
    DataModelT: TimeBasedDataModel,
](BaseFeed[SourceT, RequestT], ABC):
    DataModel: ClassVar[type[TimeBasedDataModel]]
    downloaded_data_date_cols: ClassVar[list[str]]
    # How the source's batch API is chunked: True = one dataflow per date (e.g. daily files),
    # False = one dataflow spanning the whole range (e.g. a range query API).
    download_dataflow_per_date: ClassVar[bool] = True

    @classmethod
    def _standardize_date_column(
        cls, df: pl.LazyFrame, is_raw_data: bool
    ) -> pl.LazyFrame:
        """Materialize a uniform date column for downstream filtering and dedup.

        Sources expose their date under different column names (e.g. Bybit: 'timestamp',
        Yahoo Finance: 'Datetime'/'Date'). `downloaded_data_date_cols` lists the candidates
        to look for in the input. Handling differs by data layer so raw data stays a
        faithful mirror of the source:
            - Cleaned: the source's date column is renamed to 'date'.
            - Raw: the source schema is left untouched and a '_pfeed_date' column is
            added as a copy, used by the storage handler for date-based filtering
            and dedup.

        Args:
            df: Input LazyFrame containing one of the source's date columns listed in
                `downloaded_data_date_cols`.
            is_raw_data: If True, preserve the source schema and add '_pfeed_date'.
                If False, rename the source's date column to 'date'.

        Returns:
            A LazyFrame with a normalized date column ('date' for cleaned data,
            '_pfeed_date' for raw data).

        Raises:
            ValueError: If none of the candidate source date columns are present in `df`.
        """
        from pfeed._etl.base import standardize_date_column

        cols = df.collect_schema().names()
        raw_date_col = next(
            (c for c in cls.downloaded_data_date_cols if c in cols),
            None,
        )
        if raw_date_col is None:
            raise ValueError(
                f"no date column ({cls.downloaded_data_date_cols}) found in {cols}"
            )

        if not is_raw_data:
            date_col = cls.DataModel.DATE_COL_IN_CLEANED_DATA
            df = df.rename({raw_date_col: date_col})
        else:
            date_col = cls.DataModel.DATE_COL_IN_RAW_DATA
            df = df.with_columns(pl.col(raw_date_col).alias(date_col))
        return standardize_date_column(df, date_col)

    @classmethod
    def _max_date_range(cls, resolution: Resolution) -> tuple[datetime.date, datetime.date]:
        """Resolve rollback_period='max' into the source's full available history.

        By default, the range runs from the data source's `METADATA.start_date` to yesterday,
        regardless of resolution. Override it if the source's history depends on the resolution,
        e.g. Yahoo Finance keeps only the last 8 days of minute data.

        Args:
            resolution: The requested resolution. Unused by default, available to overrides.

        Returns:
            (start_date, end_date), both inclusive.

        Raises:
            ValueError: If the data source has no `METADATA.start_date`.
        """
        
        data_source_start_date = cls.DataSource.METADATA.start_date
        if not data_source_start_date:
            raise ValueError(
                f'{cls.DataSource.METADATA.name} has no data source start_date, cannot use rollback_period="max"'
            )
        return parse_date_range(data_source_start_date)

    @classmethod
    def _standardize_dates(
        cls,
        resolution: Resolution,
        start_date: str | datetime.date | None,
        end_date: str | datetime.date | None,
        rollback_period: Resolution | str | Literal["ytd", "max"],
    ) -> tuple[datetime.date, datetime.date]:
        """Standardize start_date and end_date based on input parameters.

        Args:
            resolution: The resolution of the data, only used when rollback_period is 'max'.
            start_date: Start date, a YYYY-MM-DD string or datetime.date.
                If not provided, will be determined by rollback_period.
            end_date: End date, a YYYY-MM-DD string or datetime.date.
                If not provided and start_date is provided, defaults to yesterday.
                If not provided and start_date is not provided, will be determined by rollback_period.
            rollback_period: Period to roll back, ending yesterday, if start_date is not provided.
                Can be a period string like '1d', '1w', '1m', '1y' etc.
                Or 'ytd' to use the start date of the current year.
                Or 'max' to use the source's full history, see `_max_date_range`.

        Returns:
            tuple[datetime.date, datetime.date]: Standardized (start_date, end_date)

        Raises:
            ValueError: If end_date is given without start_date, start_date is after end_date,
                rollback_period is invalid, or rollback_period='max' but the date range can't be derived (see `_max_date_range`)
        """
        
        if rollback_period.lower() == "max" and not start_date:
            if end_date:
                raise ValueError(f"{end_date=} is set but start_date is not")
            return cls._max_date_range(resolution)
        return parse_date_range(start_date, end_date, rollback_period)

    def _create_batch_dataflows(
        self, extract_func: Callable[[DataModelT], Any]
    ) -> list[DataFlow]:
        request = self._get_current_request()
        self.logger.debug(
            f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN
        )
        data_model = cast("DataModelT", request.to_data_model())
        faucet: Faucet = self._create_faucet(
            data_source=self.data_source,
            extract_func=extract_func,
            extract_type=request.extract_type,
        )
        if request.dataflow_per_date:
            data_models = [
                data_model.model_copy(update={"start_date": d, "end_date": d})
                for d in data_model.dates
            ]
        else:
            data_models = [data_model]
        dataflows = [
            self._create_dataflow(faucet=faucet, data_model=dm) for dm in data_models
        ]
        self._dataflows[request] = dataflows
        return dataflows

    def run(self, **prefect_kwargs: Any) -> RunResult:
        """Runs dataflows and returns a RunResult exposing the combined frame,
        per-flow successes/failures, and any dates that produced no data."""
        import narwhals as nw

        from pfeed.dataflow.result import RunResult
        from pfeed.utils.dataframe import is_empty_dataframe

        result_dfs: list[IntoFrame] = []

        dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)
        for dataflow in dataflows:
            result: DataFlowResult = dataflow.result
            _df: IntoFrame | None = result.data
            if _df is not None:
                result_dfs.append(_df)

        dfs: list[Frame] = [
            nw.from_native(df) for df in result_dfs if not is_empty_dataframe(df)
        ]
        if dfs:
            from pfeed._etl.base import convert_dataframe

            df: Frame = cast("Frame", nw.concat(dfs))  # pyright: ignore[reportArgumentType]
            schema = df.collect_schema()
            columns = schema.names()
            if "date" in columns and schema["date"].is_temporal():
                df: Frame = df.sort(by="date", descending=False)
            # Convert once here so the aggregated frame is a polars LazyFrame.
            combined: IntoFrame | None = convert_dataframe(nw.to_native(df))
        else:
            combined = None

        return RunResult(data=combined, dataflows=dataflows)
