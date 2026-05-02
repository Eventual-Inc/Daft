from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

if TYPE_CHECKING:
    from daft.logical.schema import Schema
    from daft.recordbatch import MicroPartition


@dataclass(frozen=True)
class Preview:
    partition: MicroPartition | None
    total_rows: int | None


PreviewFormat = Literal[
    "fancy",
    "plain",
    "simple",
    "grid",
    "markdown",
    "html",
    # TODO "latex"
]


PreviewAlign = Literal[
    "auto",
    "left",
    "center",
    "right",
]

_SHOW_DEFAULT_VERBOSE = False
_SHOW_DEFAULT_MAX_WIDTH = 30
_SHOW_DEFAULT_ALIGN: PreviewAlign = "left"

_SHOW_FORMATS = {"fancy", "plain", "simple", "grid", "markdown", "html"}
_SHOW_ALIGNS = {"auto", "left", "center", "right"}
_SHOW_TRUTHY = {"1", "true", "yes", "on"}
_SHOW_FALSY = {"0", "false", "no", "off"}
_SHOW_NONE = {"none", "null"}


def _parse_bool_env(name: str) -> bool | None:
    value = os.environ.get(name)
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in _SHOW_TRUTHY:
        return True
    if normalized in _SHOW_FALSY:
        return False
    return None


def _parse_max_width_env(name: str) -> tuple[bool, int | None]:
    value = os.environ.get(name)
    if value is None:
        return False, None
    normalized = value.strip().lower()
    if normalized in _SHOW_NONE:
        return True, None
    try:
        parsed = int(normalized)
    except ValueError:
        return False, None
    if parsed < 1:
        return False, None
    return True, parsed


def _parse_show_format_env() -> PreviewFormat | None:
    value = os.environ.get("DAFT_SHOW_FORMAT")
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in _SHOW_FORMATS:
        return cast("PreviewFormat", normalized)
    return None


def _parse_show_align_env() -> PreviewAlign | None:
    value = os.environ.get("DAFT_SHOW_ALIGN")
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in _SHOW_ALIGNS:
        return cast("PreviewAlign", normalized)
    return None


def resolve_show_defaults(
    format: PreviewFormat | None,
    verbose: bool | None,
    max_width: int | None,
    align: PreviewAlign | None,
) -> tuple[PreviewFormat | None, bool, int | None, PreviewAlign]:
    """Resolve .show() defaults from env vars when arguments are left at defaults.

    Supported env vars:
    - DAFT_SHOW_FORMAT: fancy|plain|simple|grid|markdown|html
    - DAFT_SHOW_VERBOSE: 1/0, true/false, yes/no, on/off
    - DAFT_SHOW_MAX_WIDTH: positive integer, or none/null/off
    - DAFT_SHOW_ALIGN: auto|left|center|right
    """
    if format is None:
        env_format = _parse_show_format_env()
        if env_format is not None:
            format = env_format

    if verbose is None:
        env_verbose = _parse_bool_env("DAFT_SHOW_VERBOSE")
        verbose = env_verbose if env_verbose is not None else _SHOW_DEFAULT_VERBOSE

    if max_width is None:
        max_width_set, env_max_width = _parse_max_width_env("DAFT_SHOW_MAX_WIDTH")
        if max_width_set:
            max_width = env_max_width
        else:
            max_width = _SHOW_DEFAULT_MAX_WIDTH

    if align is None:
        env_align = _parse_show_align_env()
        align = env_align if env_align is not None else _SHOW_DEFAULT_ALIGN

    return format, verbose, max_width, align


class PreviewColumn(TypedDict, total=False):
    header: str
    info: str
    max_width: int
    align: PreviewAlign


class PreviewOptions:
    """Preview options for show formatting.

    Usage:
        - If columns are given, their length MUST match the schema.
        - If columns are given, their settings override any global settings.

    Options:
        verbose     (bool)                      : verbose will print header info
        null        (str)                       : null string, default is 'None'
        max_width   (int)                       : global max column width
        align       (PreviewAlign)              : global column align
        columns     (list[PreviewColumn]|None)  : column overrides

    """

    _DEFAULTS = {
        "verbose": False,
        "null": "None",
        "max_width": 30,
        "align": "left",
        "columns": None,
    }

    def __init__(self, **options: Any) -> None:
        self._options = {**self._DEFAULTS, **options}

    def __repr__(self) -> str:
        """For debugging."""
        return self.serialize()

    def is_default(self) -> bool:
        """Return True if options match the default preview options."""
        return self._options == self._DEFAULTS

    def serialize(self) -> str:
        """This lowers the burden to interop with the rust formatter."""
        return json.dumps(self._options)


class PreviewFormatter:
    _preview: Preview
    _schema: Schema
    _format: PreviewFormat | None
    _options: PreviewOptions

    def __init__(
        self,
        preview: Preview,
        schema: Schema,
        format: PreviewFormat | None = None,
        **options: Any,
    ) -> None:
        self._preview = preview
        self._schema = schema
        self._format = format
        self._options = PreviewOptions(**options)

    def _get_user_message(self) -> str:
        if self._preview.partition is None:
            return "(No data to display: Dataframe not materialized, use .collect() to materialize)"
        if self._preview.total_rows == 0:
            return "(No data to display: Materialized dataframe has no rows)"
        if self._preview.total_rows is None:
            first_rows = len(self._preview.partition)
            return f"(Showing first {first_rows} rows)"
        else:
            first_rows = min(self._preview.total_rows, len(self._preview.partition))
            total_rows = self._preview.total_rows
            return f"(Showing first {first_rows} of {total_rows} rows)"

    def _repr_html_(self) -> str:
        if len(self._schema) == 0:
            return "<small>(No data to display: Dataframe has no columns)</small>"
        res = "<div>\n"
        res += self._to_html()
        res += f"\n<small>{self._get_user_message()}</small>\n</div>"
        return res

    def __repr__(self) -> str:
        if len(self._schema) == 0:
            return "(No data to display: Dataframe has no columns)"
        res = self._to_text()
        res += f"\n{self._get_user_message()}"
        return res

    def _to_html(self) -> str:
        if self._preview.partition is not None:
            return self._preview.partition.to_record_batch()._repr_html_()
        else:
            return self._schema._truncated_table_html()

    def _to_text(self) -> str:
        partition = self._preview.partition
        if partition is None:
            return self._schema._truncated_table_string()

        record_batch = partition.to_record_batch()._recordbatch
        if self._format or not self._options.is_default():
            # give an error to hopefully avoid bug reports until we implement this
            if self._format == "html":
                raise ValueError("Formatting options with HTML are not currently supported.")
            return record_batch.preview(self._format, self._options.serialize())

        return repr(record_batch)

    def _generate_interactive_html(self) -> str:
        """Generate interactive HTML for the current PreviewFormatter's RecordBatch using the dashboard server."""
        from daft.daft import dashboard as dashboard_native
        from daft.subscribers import launch

        # Ensure the server is running (no-op if already running)
        launch(noop_if_initialized=True)

        # Get the RecordBatch from the current preview partition
        if self._preview.partition is None:
            raise ValueError("No partition available to generate interactive HTML.")

        rb = self._preview.partition.to_record_batch()
        df_id = dashboard_native.register_dataframe_for_display(rb._recordbatch)
        html = dashboard_native.generate_interactive_html(df_id)
        user_message = self._get_user_message()
        # Add the user message below the interactive HTML
        html += f"\n<small>{user_message}</small>"
        return html
