"""MONKEY PATCHED TABULATE."""

import tabulate
from tabulate import DataRow, Line, TableFormat

# add a default which is like our rust comfytable
tabulate._table_formats["default"] = TableFormat(
    lineabove=Line("╭", "─", "┬", "╮"),
    linebelowheader=Line("╞", "═", "╪", "╡"),
    linebetweenrows=Line("├", "╌", "┼", "┤"),
    linebelow=Line("╰", "─", "┴", "╯"),
    headerrow=DataRow("│", "┆", "│"),
    datarow=DataRow("│", "┆", "│"),
    padding=1,
    with_header_hide=None,
)

# alias markdown to github
tabulate._table_formats["markdown"] = tabulate._table_formats["github"]

# re-export now that it's patched
tabulate = tabulate.tabulate
