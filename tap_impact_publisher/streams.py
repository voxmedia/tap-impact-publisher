"""Stream type classes for tap-impact-publisher."""

import copy
import urllib.parse
from pathlib import Path
import time
from typing import Any, cast, Dict, Optional, Union, List, Iterable, Tuple

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_impact_publisher.client import impactPublisherStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AccountsStream(impactPublisherStream):
    """Define custom stream."""
    name = "accounts"
    path = "/CompanyInformation"
    primary_keys = ["CompanyName"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "accounts.schema.json"


class ReportsStream(impactPublisherStream):
    """Define custom stream."""
    name = "reports"
    path = "/Reports/mp_action_listing_sku.json"
    primary_keys = ["@uri"]
    replication_key = 'Action_Date'
    schema_filepath = SCHEMAS_DIR / "reports.schema.json"
    records_jsonpath = '$.Records[*]'

    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[pendulum.datetime]:
        """Get starting replication timestamp.

        Will return the value of the stream's replication key when `--state` is passed.
        If no state exists, will return `start_date` if set, or `None` if neither
        the stream state nor `start_date` is set.

        Developers should use this method to seed incremental processing for date
        and datetime replication keys. For non-datetime replication keys, use
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()`

        Args:
            context: Stream partition or context dictionary.

        Returns:
            `start_date` from config, or state value if using timestamp replication.

        Raises:
            ValueError: If the replication value is not a valid timestamp.
        """
        state = self.get_context_state(context)
        self.logger.info(state)
        if state:
            value = state['starting_replication_value']
        else:
            value = self.config['start_date']

        return pendulum.parse(value, tz="America/New_York")

    def _make_time_chunks(
        self,
        context: Optional[dict]
    ) -> Iterable[Tuple[int, int]]:
        """generate chunks of time to be used in URL parameterization.
           Since data can be updated up to six months after the action date, we need to
           subtract six month back from the latest replication value to get the starting date

        Args:
            context (Optional[dict]): Optional stream context.

        Returns:
            Iterable[Tuple[int, int]]: Iterable with start/end dates for the pertinent months
        """
        start = self.get_starting_timestamp(context)
        if start != pendulum.parse(self.config['start_date'], tz="America/New_York"):
            start = start.subtract(months=12).start_of('week')

        iterable = (
            (start.add(weeks=i), start.add(weeks=i).end_of("week"))
            for i in range(
                pendulum.yesterday('America/New_York').diff(
                    start
                ).in_weeks() + 1
            )
        )
        return iterable

    def get_url_params(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "SUPERSTATUS_MS": ["APPROVED", "NA", "PENDING", "REVERSED"],
            "SHOW_STATUS_DETAIL": 1,
            "SHOW_PAYSTUB": 1,
            "SHOW_SKU": 1,
            "SHOW_LOCKING_DATE": 1,
            "SHOW_MODIFICATION_DATE": 1,
            "SHOW_SCHEDULED_CLEARING_DATE": 1,
            "SHOW_AD": 1,
            "SHOW_GEO_LOCATION": 1,
            "SHOW_IP_ADDRESS": 1,
            "SHOW_REFERRAL_DATE": 1,
            "SHOW_REFERRAL_TYPE": 1,
            "SHOW_REFERRING_URL": 1,
            "SHOW_ORIGINAL_SALEAMOUNT": 1,
            "SHOW_CUSTOMER_ID": 1,
            "SHOW_ORIGINAL_PAYOUT": 1,
            "SHOW_CUSTDATE1": 1,
            "SHOW_CUSTDATE2": 1,
            "SHOW_ADVSTRING1": 1,
            "SHOW_ADVSTRING2": 1,
            "SHOW_CALL": 1,
            "SHOW_USER_AGENT2": 1,
            "SHOW_CURRENCY_CONV": 1,
            "START_DATE": start_date,
            "END_DATE": end_date
        }
        if next_page_token:
            return urllib.parse.parse_qs(urllib.parse.urlparse(next_page_token).query)
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request(
        self, start_date, end_date, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        self.logger.info(
            f"Making params for {start_date.to_date_string()} to {end_date.to_date_string()}"
        )
        params: dict = self.get_url_params(
            start_date=start_date.format('YYYY-MM-DD'),
            end_date=end_date.format('YYYY-MM-DD'),
            next_page_token=next_page_token,
        )
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
            params.update(authenticator.auth_params or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )
        self.logger.info(f'requesting url :{request.url}')
        return request

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        decorated_request = self.request_decorator(self._request)

        for start, end in self._make_time_chunks(context):
            finished = False
            self.logger.info(f"Fetching {self.name} from {start} to {end}")

            while not finished:
                prepared_request = self.prepare_request(
                    start_date=start,
                    end_date=end,
                    context=context,
                    next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp):
                    yield row
                time.sleep(1)
                self.finalize_state_progress_markers(
                    {"bookmarks": {self.name: {"replication_key_value": end}}}
                )
                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                self.logger.info(f"L225: Next page token is {next_page_token}")
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                # Cycle until get_next_page_token() no longer returns a value
                finished = not next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
