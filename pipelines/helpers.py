import click
from dlt.common.time import ensure_pendulum_date
from dlt.common.typing import TDataItem
from pendulum import Date
from typing import Iterable, Tuple


def get_eod_bulk(client, exchange, date: str = None) -> Iterable[TDataItem]:
    yield client.get_eod_bulk_last_day(exchange=exchange, date=date)


def get_dates() -> Tuple[Date, Date]:
    start_date_str = click.prompt(
        "What date would you like to start from? (YYYY-MM-DD)",
        type=str,
        show_default=True,
    ).strip()

    start_date = ensure_pendulum_date(start_date_str)

    while True:
        end_date_str = click.prompt(
            "Enter end date (YYYY-MM-DD) or press Enter to skip",
            default="",
            show_default=False,
        ).strip()

        if not end_date_str:
            end_date = Date.today().subtract(days=1)
            break
        else:
            end_date = ensure_pendulum_date(end_date_str)

            if end_date is None:
                click.echo("Invalid end date format. Try again.")
            elif end_date <= start_date:
                click.echo("End date should be after the start date. Try again.")
            else:
                break

    return start_date, end_date
