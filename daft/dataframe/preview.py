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
    "default",
    "plain",
    "simple",
    "grid",
    "markdown",
    "latex",
]


class PreviewOptions(TypedDict, total=False):
    schema: bool
    null: str
    max_width: int | list[int]
    align: str | list[str]


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
            return f"(Showing first {min(self._preview.num_rows, len(self._preview.partition))} rows)"
        return f"(Showing first {min(self._preview.num_rows, len(self._preview.partition))} of {self._preview.num_rows} rows)"

    def _repr_html_(self) -> str:
        if len(self._schema) == 0:
            return "<small>(No data to display: Dataframe has no columns)</small>"
        res = "<div>\n"
        if self._preview.partition is not None:
            res += self._preview.partition.to_record_batch()._repr_html_()
        else:
            res += self._schema._truncated_table_html()
        res += f"\n<small>{self._get_user_message()}</small>\n</div>"
        return res

    def __repr__(self) -> str:
        if len(self._schema) == 0:
            return "(No data to display: Dataframe has no columns)"
        if self._preview.partition is not None:
            preview = self._preview.partition.to_record_batch()
            if self._format or self._options:
                # TODO format via rust
                raise ValueError("format options are not yet supported")
            else:
                res = preview.__repr__()
        else:
            res = self._preview.schema()._truncated_table_string()
        res += f"\n{self._get_user_message()}"
        return res
