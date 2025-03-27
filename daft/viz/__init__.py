from __future__ import annotations

from typing import Literal
from .dataframe_display import DataFrameDisplay
from .html_viz_hooks import register_viz_hook

__all__ = ["DataFrameDisplay", "register_viz_hook"]

ShowFormat = Literal[
    "default",
    "plain",
    "simple",
    "asciidoc",
    "markdown",
    "github",
    "jira",
    "mediawiki",
    "html",
    "latex",
    "csv",
    "tsv",
    "rst",
]
