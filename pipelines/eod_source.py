import dlt

from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from pendulum import Date
from typing import Iterator, Optional, Sequence

from adaptors import EODAPIClient
from .helpers import get_eod_bulk

us_stocks_pipeline = dlt.pipeline(
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
    end_date: Optional[Date] = None,
) -> Sequence[DltResource]:
    api_client = EODAPIClient(api_key=eodhd_api_key)

    start_date = Date(2023, 10, 1) if initial_start_date is None else initial_start_date
    end_date = get_latest_date() if end_date is None else end_date

    @dlt.resource(
        name="us_stocks_latest",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def us_stocks_latest(
        date: Optional[Date] = None,
    ) -> Iterator[TDataItem]:
        yield get_eod_bulk(client=api_client, exchange="US", date=date)

    @dlt.resource(
        name="us_stocks_historical",
        primary_key=["code", "exchange_short_name", "date"],
        write_disposition="merge",
    )
    def us_stocks_historical(
        date=dlt.sources.incremental[str](
            "date",
            initial_value=start_date.to_date_string(),
            end_value=end_date.to_date_string(),
        )
    ) -> Iterator[TDataItem]:
        # FIXME: unable to get incremental to work
        for data in get_eod_bulk(
            client=api_client, exchange="US", date=date.last_value
        ):
            yield data

            if date.end_out_of_range:
                return

    return (
        us_stocks_latest,
        us_stocks_historical,
    )
