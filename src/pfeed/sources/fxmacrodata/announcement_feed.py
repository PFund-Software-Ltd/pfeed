from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from json import loads
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

import polars as pl

from pfeed.sources.fxmacrodata.mixin import FXMacroDataMixin

JsonRequest = Callable[[str, Mapping[str, str]], Mapping[str, Any]]


class _NoRedirectHandler(HTTPRedirectHandler):
    # urllib copies request headers onto the redirected request, which would
    # send X-API-Key to whatever host the redirect points at
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = build_opener(_NoRedirectHandler)


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
            with _opener.open(request, timeout=30) as response:
                body = response.read()
        except HTTPError as err:
            if 300 <= err.code < 400:
                raise RuntimeError(
                    f"FXMacroData request was redirected (HTTP {err.code}); "
                    "redirects are not followed"
                ) from None
            body = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"FXMacroData request failed with HTTP {err.code}: {body[:500]}"
            ) from err
        try:
            return loads(body.decode("utf-8"))
        except ValueError:
            raise RuntimeError(
                "FXMacroData returned a response that is not JSON"
            ) from None

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
        api_key = (self._api_key or "").strip()
        if api_key:
            if any(char.isspace() or not char.isprintable() for char in api_key):
                # the message must not echo the key
                raise ValueError(
                    "FXMacroData API key contains whitespace or control characters"
                )
            headers["X-API-Key"] = api_key

        path = f"{quote(currency.upper())}/{quote(indicator)}"
        rows: list[dict[str, Any]] = []
        metadata: dict[str, Any] = {}
        while True:
            url = f"{self.API_ROOT}/announcements/{path}?{urlencode(parameters)}"
            payload = self._request_json(url, headers)
            if not isinstance(payload, Mapping):
                raise ValueError(
                    "FXMacroData announcements response is not a JSON object"
                )
            data = payload.get("data")
            if not isinstance(data, list):
                detail = payload.get("detail")
                raise ValueError(
                    "FXMacroData announcements response contains non-list data"
                    + (f": {detail}" if isinstance(detail, str) else "")
                )
            if not all(isinstance(row, Mapping) for row in data):
                raise ValueError(
                    "FXMacroData announcements response contains non-object rows"
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
