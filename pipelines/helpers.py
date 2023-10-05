import click
from datetime import datetime, timedelta
from dlt.common.typing import TDataItem
from typing import Iterable, Tuple


def get_eod_bulk(client, exchange, date_str: str = None) -> Iterable[TDataItem]:
    date = None

    if date_str:
        date = validate_date(date_str)

    yield client.get_eod_bulk_last_day(exchange=exchange, date=date)


def get_dates() -> Tuple[datetime.date, datetime.date]:
    start_date_str = click.prompt(
        "What date would you like to start from? (YYYY-MM-DD)",
        type=str,
        show_default=True,
    ).strip()

    start_date = validate_date(start_date_str)

    while True:
        end_date_str = click.prompt(
            "Enter end date (YYYY-MM-DD) or press Enter to skip",
            default="",
            show_default=False,
        ).strip()
        if not end_date_str:
            end_date = datetime.today().date() - timedelta(days=1)
            break
        else:
            end_date = validate_date(end_date_str)
            if end_date is None:
                click.echo("Invalid end date format. Try again.")
            elif end_date <= start_date:
                click.echo("End date should be after the start date. Try again.")
            else:
                break

    return start_date, end_date


def validate_date(date_str: str) -> datetime.date:
    """Validates the date in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
