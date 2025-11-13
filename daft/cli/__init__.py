"""Daft CLI package."""

import click
from rich.console import Console

console = Console()


@click.group()  # type: ignore[misc]
@click.version_option()  # type: ignore[misc]
def cli() -> None:
    """Command-line tools for Daft."""
    pass


# Import commands to register them with the CLI group
from daft.cli.commands import init

cli.add_command(init.init)


def main() -> None:
    """Main entry point for the CLI."""
    cli()


__all__ = ["main"]
