from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypedDict

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

    _options: dict[str, object]  # normalized options

    def __init__(self, **options: Any) -> None:
        self._options = {
            "verbose": options.get("verbose", False),
            "null": options.get("null", "None"),
            "max_width": options.get("max_width", 30),
            "align": options.get("align", "left"),
            "columns": options.get("columns"),
        }

    def __repr__(self) -> str:
        """For debugging."""
        return self.serialize()

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
        # give an error to hopefully avoid bug reports until we implement this
        if self._format == "html" and self._options:
            raise ValueError("Formatting options with HTML are not currently supported.")

        if self._preview.partition is not None:
            if self._format:
                return self._preview.partition.to_record_batch()._recordbatch.preview(
                    self._format, self._options.serialize()
                )
            else:
                return self._preview.partition.to_record_batch()._recordbatch.__repr__()
        else:
            return self._schema._truncated_table_string()

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
