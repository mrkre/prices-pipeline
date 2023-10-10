import dlt

from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from pendulum import Date
from typing import Iterator, Optional, Sequence

from adaptors import EODAPIClient
from .helpers import get_eod_bulk

market_data_pipeline = dlt.pipeline(
    pipeline_name="eodhd_pipeline",
    destination="duckdb",
    dataset_name="market_data",
    loader_file_format="parquet",
    progress=dlt.progress.enlighten(colour="yellow"),
)


primary_key = ["code", "exchange_short_name", "date"]


def get_latest_date():
    end_date = Date.today()

    return end_date


@dlt.source(name="eodhd")
def eodhd(
    eodhd_api_key: str = dlt.secrets.value,
    initial_start_date: Optional[Date] = None,
) -> Sequence[DltResource]:
    api_client = EODAPIClient(api_key=eodhd_api_key)

    start_date = initial_start_date or get_latest_date()

    start_date_str = start_date.to_date_string()

    @dlt.resource(
        name="us_stocks",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="append",
    )
    def us_stocks(
        date=dlt.sources.incremental[str](
            "date",
            initial_value=start_date_str,
        )
    ) -> Iterator[TDataItem]:
        for data in get_eod_bulk(
            client=api_client, exchange="US", date=date.start_value
        ):
            yield data

    @dlt.resource(
        name="forex",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="append",
    )
    def forex(
        date=dlt.sources.incremental[str](
            "date",
            initial_value=start_date_str,
        )
    ) -> Iterator[TDataItem]:
        for data in get_eod_bulk(
            client=api_client, exchange="FOREX", date=date.start_value
        ):
            yield data

    return (
        us_stocks,
        forex,
    )
