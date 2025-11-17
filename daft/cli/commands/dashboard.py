"""Dashboard command for Daft CLI."""

from __future__ import annotations

import click


@click.command()  # type: ignore[misc]
@click.help_option("-h", "--help")  # type: ignore[misc]
def dashboard() -> None:
    """Start the Daft dashboard server."""
    pass
