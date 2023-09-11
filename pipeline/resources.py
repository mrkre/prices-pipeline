import dlt
from adaptors import EODAPIClient


@dlt.resource(
    primary_key=["code", "exchange_short_name", "date"], write_disposition="merge"
)
def update_us_stocks(
    api_client: EODAPIClient,
    date: None,
):
    exchange = "US"

    print(f"Fetching {exchange} stocks for {date}...")

    response = api_client.get_eod_bulk_last_day(exchange=exchange, date=date)

    if response.status_code == 200:
        print(f"Fetched {exchange} stocks for {date}")

        return response.json()
    else:
        raise Exception(
            f"Error fetching {exchange} stocks for {date} - {response.status_code} - {response.text}"
        )
