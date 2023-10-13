import click

from pipelines.eod_source import market_data_pipeline, eodhd
from pipelines.helpers import get_yesterday, get_dates, generate_dates


def handle_us_stocks_update(sub_choice):
    click.echo("Choose update option:")

    update_choices = ["1", "2"]

    if not sub_choice:
        while True:
            sub_choice = click.prompt(
                "What data would you like to update? ([1] Latest [2] Historical)",
                type=click.Choice(update_choices, case_sensitive=False),
                show_choices=False,
                show_default=False,
                default="1",
            )

            if sub_choice in update_choices:
                break
            else:
                click.echo("Invalid choice.")

    if sub_choice == "1":
        click.echo("Updating latest US stocks data.")

        start_date = get_yesterday()

        data = eodhd(initial_start_date=start_date).with_resources("us_stocks")

        info = market_data_pipeline.run(data)

        click.echo(info)
    elif sub_choice == "2":
        start_date, end_date = get_dates()

        dates = generate_dates(start_date, end_date)

        click.echo(
            f"Updating historical US stocks data from {start_date} to {end_date}."
        )

        for idx, date in enumerate(dates):
            initial_start_date = date
            next_date = dates[idx + 1] if idx < len(dates) - 1 else None

            data = eodhd(
                initial_start_date=initial_start_date, end_date=next_date
            ).with_resources("us_stocks")

            info = market_data_pipeline.run(data)

            click.echo(info)


def handle_data_update(choice, sub_choice):
    if choice == "1":
        handle_us_stocks_update(sub_choice)
    elif choice == "2":
        click.echo("Not implemented.")


@click.command()
@click.option(
    "--choice",
    "-c",
    type=str,
    help="Data update - US Stocks or Market. If provided, skips the interactive prompt for this.",
)
@click.option(
    "--sub-choice",
    "-s",
    type=str,
    help="Sub-choice Latest or Historical data. Relevant only if --choice is US Stocks. If provided, skips the "
    "interactive prompt for this.",
)
def main(choice, sub_choice):
    click.echo("Welcome to the EODHD data pipeline.")

    if not choice:
        choice = click.prompt(
            "What data would you like to update? ([1] US Stocks [2] Market)",
            type=click.Choice(["1", "2"], case_sensitive=False),
            show_choices=False,
            show_default=False,
            default="1",
        )
    handle_data_update(choice, sub_choice)


if __name__ == "__main__":
    main()
