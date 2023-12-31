import dlt

from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from pendulum import Date
from typing import Iterator, Optional, Sequence

from adaptors import EODAPIClient
from .helpers import get_eod_bulk, get_today

market_data_pipeline = dlt.pipeline(
    pipeline_name="eodhd_pipeline",
    destination="duckdb",
    dataset_name="market_data",
    loader_file_format="parquet",
    progress=dlt.progress.enlighten(colour="yellow"),
)


primary_key = ["code", "exchange_short_name", "date"]


@dlt.source(name="eodhd")
def eodhd(
    eodhd_api_key: str = dlt.secrets.value,
    initial_start_date: Optional[Date] = None,
    end_date: Optional[Date] = None,
) -> Sequence[DltResource]:
    api_client = EODAPIClient(api_key=eodhd_api_key)

    start_date = initial_start_date or get_today()

    start_date_str = start_date.to_date_string()

    end_date_str = end_date.to_date_string() if end_date else None

    @dlt.resource(
        name="us_stocks",
        table_name="us_stocks",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def us_stocks(
        date=dlt.sources.incremental[str](
            "date", initial_value=start_date_str, end_value=end_date_str
        )
    ) -> Iterator[TDataItem]:
        print(f"Getting bulk data for US on {date.start_value}.")

        for data in get_eod_bulk(
            client=api_client, exchange="US", date=date.start_value
        ):
            yield data

            if date.start_out_of_range or date.end_out_of_range:
                break

    @dlt.resource(
        name="forex",
        table_name="forex",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def forex(
        date=dlt.sources.incremental[str](
            "date", initial_value=start_date_str, end_value=end_date_str
        )
    ) -> Iterator[TDataItem]:
        print(f"Getting bulk data for FOREX on {date.start_value}.")

        for data in get_eod_bulk(
            client=api_client, exchange="FOREX", date=date.start_value
        ):
            yield data

            if date.end_out_of_range:
                break

    return (
        us_stocks,
        forex,
    )
