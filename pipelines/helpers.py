import click
import holidays

from dlt.common.time import ensure_pendulum_date
from dlt.common.typing import TDataItem
from pendulum import Date
from typing import Iterable, List, Tuple


def get_eod_bulk(client, exchange, date: str = None) -> Iterable[TDataItem]:
    yield client.get_eod_bulk_last_day(exchange=exchange, date=date)


def get_today():
    end_date = Date.today()

    return end_date


def get_yesterday() -> Date:
    return Date.today().subtract(days=1)


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
            end_date = get_today()
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


def generate_dates(
    start_date: Date, end_date: Date, exclude_holidays: str = None
) -> List[Date]:
    holidays_list = []

    if exclude_holidays:
        if exclude_holidays in ["NYSE", "ECB"]:
            holidays_list = holidays.financial_holidays(exclude_holidays)
        else:
            holidays_list = holidays.country_holidays(exclude_holidays)

    dates = []

    current_date = start_date

    while current_date <= end_date:
        if current_date not in holidays_list and current_date.isoweekday() < 6:
            dates.append(current_date)

        current_date = current_date.add(days=1)

    return dates
