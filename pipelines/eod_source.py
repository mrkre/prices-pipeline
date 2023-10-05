import dlt

from datetime import datetime
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from typing import Iterator, Optional, Sequence

from adaptors import EODAPIClient
from .helpers import get_eod_bulk

us_stocks_pipeline = dlt.pipeline(
    pipeline_name="eodhd_pipeline",
    destination="duckdb",
    dataset_name="us_stocks",
    loader_file_format="parquet",
)


primary_key = ["code", "exchange_short_name", "date"]


def get_latest_date():
    end_date = datetime.now().date()

    return end_date.strftime("%Y-%m-%d")


@dlt.source(name="eodhd")
def eodhd(
    eodhd_api_key: str = dlt.secrets.value,
) -> Sequence[DltResource]:
    api_client = EODAPIClient(api_key=eodhd_api_key)

    @dlt.resource(
        name="us_stocks_latest",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def us_stocks_latest(
        date_str: Optional[str] = None,
    ) -> Iterator[TDataItem]:
        yield get_eod_bulk(client=api_client, exchange="US", date_str=date_str)

    @dlt.resource(
        name="us_stocks_historical",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def us_stocks_historical(
        date=dlt.sources.incremental(
            "data", initial_value="2023-10-01", last_value_func=get_latest_date
        ),
    ) -> Iterator[TDataItem]:
        for data in get_eod_bulk(
            client=api_client, exchange="US", date_str=date.start_value
        ):
            yield data

    return (
        us_stocks_latest,
        us_stocks_historical,
    )
