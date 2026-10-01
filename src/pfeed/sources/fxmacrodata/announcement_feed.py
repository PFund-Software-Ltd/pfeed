from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from json import loads
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import polars as pl

from pfeed.sources.fxmacrodata.mixin import FXMacroDataMixin

JsonRequest = Callable[[str, Mapping[str, str]], Mapping[str, Any]]


class FXMacroDataAnnouncementFeed(FXMacroDataMixin):
    """Download realised FXMacroData announcement records.

    The returned frame keeps the endpoint's announcement fields as they are.
    PFeed adds no replacement economic schema and does not mix in forward-looking
    calendar events.
    """

    API_ROOT = "https://api.fxmacrodata.com/v1"
    MAX_LIMIT = 100

    def __init__(
        self,
        pipeline_mode: bool = False,
        num_workers: int | None = None,
        api_key: str | None = None,
        request_json: JsonRequest | None = None,
    ):
        self._pipeline_mode = pipeline_mode
        self._num_workers = num_workers
        self.data_source = self._create_data_source()
        # FXMACRODATA_API_KEY / FXMD_API_KEY are read by the data source
        self._api_key = api_key or self.data_source._api_key
        self._request_json = request_json or self._get_json
        self.last_response_metadata: dict[str, Any] = {}

    @property
    def name(self):
        return self.data_source.name

    @staticmethod
    def _get_json(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
        request = Request(url, headers=dict(headers))
        try:
            with urlopen(request, timeout=30) as response:
                return loads(response.read().decode("utf-8"))
        except HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"FXMacroData request failed with HTTP {err.code}: {body[:500]}"
            ) from err

    def download(
        self,
        currency: str,
        indicator: str,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        limit: int = MAX_LIMIT,
    ) -> pl.LazyFrame:
        """Return realised announcements with FXMacroData's own columns.

        Without an API key only USD is available, limited to the most recent
        90 days and delayed by 15 minutes. Set ``FXMACRODATA_API_KEY`` (or
        ``FXMD_API_KEY``), or pass ``api_key`` to the feed, for other currencies
        and full history. The key is sent in the ``X-API-Key`` header.

        Every page is fetched; ``limit`` is the page size (1 to 100). Response
        metadata from the last page, including ``freemium_window`` and
        ``freemium_delay`` when they apply, is kept in ``last_response_metadata``.
        """
        if not 0 < limit <= self.MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {self.MAX_LIMIT}")

        parameters: dict[str, str | int] = {"limit": limit, "offset": 0}
        if start_date:
            parameters["start_date"] = str(start_date)
        if end_date:
            parameters["end_date"] = str(end_date)

        headers = {
            "Accept": "application/json",
            "User-Agent": "pfeed-fxmacrodata",
        }
        if self._api_key:
            headers["X-API-Key"] = self._api_key

        path = f"{quote(currency.upper())}/{quote(indicator)}"
        rows: list[dict[str, Any]] = []
        metadata: dict[str, Any] = {}
        while True:
            url = f"{self.API_ROOT}/announcements/{path}?{urlencode(parameters)}"
            payload = self._request_json(url, headers)
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise ValueError(
                    "FXMacroData announcements response contains non-list data"
                )
            rows.extend(data)
            metadata = {key: value for key, value in payload.items() if key != "data"}
            pagination = payload.get("pagination") or {}
            if not data or not pagination.get("has_more", False):
                break
            next_offset = pagination.get("next_offset")
            parameters["offset"] = (
                int(next_offset)
                if next_offset is not None
                else int(parameters["offset"]) + len(data)
            )

        self.last_response_metadata = metadata
        if not rows:
            return pl.DataFrame().lazy()
        return pl.from_dicts(rows, infer_schema_length=None).lazy()


# the class name create_feed() derives from DataSource.FXMACRODATA
FxmacrodataAnnouncementFeed = FXMacroDataAnnouncementFeed
