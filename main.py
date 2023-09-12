import click

from pipelines import update_us_stocks_pipeline


def handle_us_stocks_update(sub_choice):
    click.echo("Choose update option:")

    update_choices = ["Latest", "Historical"]

    if not sub_choice:
        while True:
            sub_choice = click.prompt(
                "What data would you like to update?",
                type=click.Choice(update_choices, case_sensitive=False),
                show_choices=True,
                default="Latest",
            )

            sub_choice = sub_choice.lower()

            if sub_choice in [u.lower() for u in update_choices]:
                click.echo("Updating latest US stocks data.")
                break
            else:
                click.echo("Invalid choice.")

    if sub_choice == "latest":
        click.echo("Updating latest US stocks data.")
        update_us_stocks_pipeline()
    elif sub_choice == "historical":
        click.echo("Not implemented.")


def handle_data_update(choice, sub_choice):
    choice = choice.lower()

    if choice == "us stocks":
        handle_us_stocks_update(sub_choice)
    elif choice == "market":
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
            "What data would you like to update?",
            type=click.Choice(["US Stocks", "Market"], case_sensitive=False),
            show_choices=True,
            default="US Stocks",
        )
    handle_data_update(choice, sub_choice)


if __name__ == "__main__":
    main()
