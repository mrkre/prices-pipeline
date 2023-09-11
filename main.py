import click


def update_data(choice):
    if choice == "US Stocks":
        print("Updating US stocks data.")
    elif choice == "Market":
        print("Not implemented.")


@click.command()
def main():
    click.echo("Welcome to the EODHD data pipeline.")
    choice = click.prompt(
        "What data would you like to update?",
        type=click.Choice(["US Stocks", "Market"]),
        show_choices=True,
    )
    update_data(choice)


if __name__ == "__main__":
    main()
