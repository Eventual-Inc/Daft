from __future__ import annotations

from dataclasses import dataclass

from daft.table import Table


def is_interactive_session():
    """Helper utility to return whether or not the current Python session is an interactive
    iPython session
    """
    is_interactive = False
    try:
        from IPython import get_ipython

        is_interactive = get_ipython() is not None
    except ImportError:
        pass
    return is_interactive


@dataclass(frozen=True)
class DataFramePreview:
    """A class containing all the metadata/data required to preview a dataframe."""

    preview_partition: Table | None
    dataframe_num_rows: int | None
