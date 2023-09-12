import dlt

from adaptors import EODAPIClient
from typing import Optional


@dlt.source(name="eodhd")
def eod_source(api_key: str = dlt.secrets.value, date: Optional[str] = None):
    return [
        us_stocks(api_key=api_key, date=date),
    ]


@dlt.resource(
    name="us_stocks",
    primary_key=["code", "exchange_short_name", "date"],
    write_disposition="merge",
)
def us_stocks(
    api_key: str = dlt.secrets.value,
    date: Optional[str] = None,
):
    api_client = EODAPIClient(api_key=api_key)

    exchange = "US"

    print(f"Fetching {exchange} stocks for {date}...")

    response = api_client.get_eod_bulk_last_day(exchange=exchange, date=date)

    if response.status_code == 200:
        print(f"Fetched {exchange} stocks for {date}")

        yield from response.json()
    else:
        raise Exception(
            f"Error fetching {exchange} stocks for {date} - {response.status_code} - {response.text}"
        )
