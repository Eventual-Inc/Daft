from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.datatype import DataType as dt
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.dataframe.preview import DataFramePreview


@dataclass(frozen=True)
class DataFrameDisplay:
    preview: DataFramePreview
    schema: Schema
    # These formatting options are deprecated for now and not guaranteed to be supported.
    column_char_width: int = 20
    max_col_rows: int = 3
    num_rows: int = 10

    def _get_user_message(self) -> str:
        if self.preview.preview_partition is None:
            return "(No data to display: Dataframe not materialized)"
        if self.preview.dataframe_num_rows == 0:
            return "(No data to display: Materialized dataframe has no rows)"
        if self.preview.dataframe_num_rows is None:
            return f"(Showing first {min(self.num_rows, len(self.preview.preview_partition))} rows)"
        return f"(Showing first {min(self.num_rows, len(self.preview.preview_partition))} of {self.preview.dataframe_num_rows} rows)"

    def _repr_html_(self) -> str:
        if len(self.schema) == 0:
            return "<small>(No data to display: Dataframe has no columns)</small>"

        res = "<div>\n"

        if self.preview.preview_partition is not None:
            res += self.preview.preview_partition.to_record_batch()._repr_html_()
        else:
            res += self.schema._truncated_table_html()

        res += f"\n<small>{self._get_user_message()}</small>\n</div>"

        return res

    def __repr__(self) -> str:
        if len(self.schema) == 0:
            return "(No data to display: Dataframe has no columns)"

        if self.preview.preview_partition is not None:
            res = repr(self.preview.preview_partition.to_record_batch())
        else:
            res = self.schema._truncated_table_string()

        res += f"\n{self._get_user_message()}"

        return res

    def _format(self, format: str = "default", **options) -> str:
        # check we have our patched tabulate, otherwise _format won't work.
        try:
            from daft.viz.tabulate import tabulate
        except ImportError as ex:
            raise ValueError(f"The format `{format}` requires tabulate, please use `pip install tabulate`.") from ex

        # formatting options
        opt_schema: bool = options.get("schema", False)
        opt_null: str = options.get("null", "None")
        opt_max_width: int = options.get("max_width", 30)
        opt_align: str | list[str] | None = options.get("align", "left")

        # need an iterable
        if isinstance(opt_align, str):
            opt_align = [opt_align for _ in self.schema]
        elif len(opt_align) != len(self.schema):
            raise ValueError(
                f"option 'align' had {len(opt_align)} fields, but the schema has {len(self.schema)} fields."
            )

        # truncate the value itself, tabulate will wrap which we don't want
        def _truncate(val) -> str:
            if opt_max_width and isinstance(val, str) and len(val) > opt_max_width:
                return val[: opt_max_width - 1] + "…"
            else:
                return val

        if self.preview.preview_partition is not None:
            # need a str schema to get correct reprs
            schema = Schema._from_field_name_and_types([(f.name, dt.string()) for f in self.schema])
            preview = self.preview.preview_partition.cast_to_schema(schema)

            # convert the micropartition into a list[list[obj]]
            py_dicts = preview.to_pylist()
            py_table = [[_truncate(val or opt_null) for val in dic.values()] for dic in py_dicts]

            # include the type only if schema=True
            if opt_schema:
                headers = [f"{f.name} ({f.dtype})" for f in self.schema]
            else:
                headers = self.schema.column_names()

            # tabulate does the hard work
            res = tabulate(
                tabular_data=py_table,
                headers=headers,
                tablefmt=format,
                maxcolwidths=opt_max_width,
                colalign=opt_align,
                missingval=opt_null,
            )
        else:
            res = self.schema._truncated_table_string()

        res += f"\n\n{self._get_user_message()}"

        return res
