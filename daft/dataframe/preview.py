from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.recordbatch import MicroPartition


@dataclass(frozen=True)
class Preview:
    partition: MicroPartition | None
    num_rows: int | None


PreviewFormat = Literal[
    "fancy",
    "plain",
    "simple",
    "grid",
    "markdown",
    "latex",
    "html",
]


class PreviewOptions:
    _normalized: dict[str,any]

    def __init__(self) -> None:
        raise ValueError("Cannot call __init__ directly.")

    @staticmethod
    def from_options(schema: Schema, **options) -> PreviewOptions:
        pass

    def serialize() -> str:
        pass


class PreviewFormatter:
    _preview: Preview | None
    _schema: Schema
    _format: PreviewFormat
    _options: PreviewOptions

    def __init__(
        self,
        preview: Preview,
        schema: Schema,
        format: PreviewFormat = None,
        options: PreviewOptions = None,
    ) -> None:
        self._preview = preview
        self._schema = schema
        self._format = format
        self._options = options

    def _get_user_message(self) -> str:
        if self._preview.partition is None:
            return "(No data to display: Dataframe not materialized)"
        if self._preview.num_rows == 0:
            return "(No data to display: Materialized dataframe has no rows)"
        if self._preview.num_rows is None:
            first_rows = min(self._preview.num_rows, len(self._preview.partition))
            return f"(Showing first {first_rows} rows)"
        else:
            first_rows = min(self._preview.num_rows, len(self._preview.partition))
            total_rows = self._preview.num_rows
            return f"(Showing first {first_rows} of {total_rows} rows)"

    def _repr_html_(self) -> str:
        if len(self._schema) == 0:
            return f"<small>(No data to display: Dataframe has no columns)</small>"
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
        if self._preview.partition is not None:
            if self._options:
                return self._preview.partition.to_record_batch()._table.preview(self._format, self._options.serialize())
            elif self._format:
                return self._preview.partition.to_record_batch()._table.preview(self._format, None)
            else:
                return self._preview.partition.to_record_batch().__repr__()
        else:
            return self._schema._truncated_table_string()
