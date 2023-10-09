import click

from pipelines.eod_source import us_stocks_pipeline, eodhd
from pipelines.helpers import get_dates


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

        info = us_stocks_pipeline.run(
            eodhd(),
            table_name="us_stocks",
        )

        click.echo(info)
    elif sub_choice == "2":
        start_date, end_date = get_dates()

        click.echo(
            f"Updating historical US stocks data from {start_date} to {end_date}."
        )

        data = eodhd(initial_start_date=start_date, end_date=end_date).with_resources(
            "us_stocks_historical"
        )

        info = us_stocks_pipeline.run(
            data,
            table_name="us_stocks",
        )
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
