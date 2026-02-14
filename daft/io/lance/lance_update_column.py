from __future__ import annotations

from typing import TYPE_CHECKING, Any

import daft
import daft.pickle

# mypy: disable-error-code="import-untyped"
from daft.datatype import DataType
from daft.udf import udf as legacy_udf

if TYPE_CHECKING:
    import pathlib

    import lance


_FRAGMENT_UPDATE_HANDLER_RETURN_DTYPE = DataType.struct(
    {"fragment_meta": DataType.binary(), "fields_modified": DataType.binary()}
)


@legacy_udf(return_dtype=_FRAGMENT_UPDATE_HANDLER_RETURN_DTYPE)
class GroupFragmentUpdateUDF:
    def __init__(
        self,
        ds_uri: str,
        open_kwargs: dict[str, Any] | None = None,
        left_on: str = "_rowid",
        right_on: str | None = None,
        read_columns: list[str] | None = None,
        batch_size: int | None = 9223372036854775807,
    ):
        """Per-group update handler that invokes Lance fragment.update_columns.

        Args:
            ds_uri: URI of the Lance dataset.
            open_kwargs: Keyword arguments to open the Lance dataset.
            left_on: Key column on the Lance fragment (default "_rowid").
            right_on: Key column name present in the provided reader data (defaults to left_on).
            read_columns: Names for columns provided to the handler (must include right_on).
            batch_size: Optional batch size when building RecordBatchReader from the provided data.
        """
        import lance

        self.lance_ds = lance.dataset(ds_uri, **(open_kwargs or {}))
        self.left_on = left_on
        self.right_on = right_on or left_on
        self.read_columns = read_columns or []
        self.batch_size = batch_size

    def __call__(self, *cols: Any) -> list[dict[str, bytes]]:
        from daft.dependencies import pa as _pa

        if len(cols) == 0:
            return []

        # Last argument is the fragment_id series, preceding args are data columns as per read_columns
        *data_cols, fragment_ids = cols

        # In map_groups, all fragment_ids in this batch are the same
        ids = fragment_ids.to_pylist() if hasattr(fragment_ids, "to_pylist") else list(fragment_ids)
        if len(ids) == 0:
            return []
        frag_id = ids[0]

        # Extract rows for this fragment (all rows in the group)
        arrays: list[_pa.Array] = []
        for col_name, s in zip(self.read_columns, data_cols):
            pylist = s.to_pylist()

            if col_name == self.right_on:
                # Enforce join key types: UInt64 for "_rowid", Int64 for other integer keys.
                if self.right_on == "_rowid":
                    key_arr = _pa.array(pylist, type=_pa.uint64())
                else:
                    pylist_int = [None if v is None else int(v) for v in pylist]
                    key_arr = _pa.array(pylist_int, type=_pa.int64())
                arrays.append(key_arr)
            else:
                arr = _pa.array(pylist)
                if _pa.types.is_integer(arr.type):
                    arrays.append(arr.cast(_pa.int64()))
                else:
                    arrays.append(arr)

        tbl = _pa.Table.from_arrays(arrays, names=self.read_columns)

        batches = tbl.to_batches(max_chunksize=self.batch_size) if self.batch_size is not None else tbl.to_batches()
        reader = _pa.RecordBatchReader.from_batches(tbl.schema, batches)

        fragment = self.lance_ds.get_fragment(frag_id)
        fragment_meta, fields_modified = fragment.update_columns(reader, left_on=self.left_on, right_on=self.right_on)

        res = {
            "fragment_meta": daft.pickle.dumps(fragment_meta),
            "fields_modified": daft.pickle.dumps(fields_modified),
        }

        # Return a single result for the entire group
        return [res]


def update_columns_from_df(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    read_columns: list[str] | None = None,
    open_kwargs: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    left_on: str | None = "_rowid",
    right_on: str | None = None,
    batch_size: int | None = 9223372036854775807,
) -> lance.LanceDataset:
    import lance

    # Validate required keys
    if "fragment_id" not in df.column_names:
        raise ValueError("DataFrame must contain 'fragment_id' column for row-level update workflow")

    effective_left_on = left_on or "_rowid"
    join_key = right_on or effective_left_on

    if join_key not in df.column_names:
        hint = (
            " Read with default_scan_options={'with_row_id': True} to expose '_rowid'." if join_key == "_rowid" else ""
        )
        raise ValueError(f"DataFrame must contain join key column '{join_key}'.{hint}")

    meta_columns = {"_rowid", "_rowaddr"}

    # Compute dataset existing field names robustly
    existing_fields: set[str] = set()
    try:
        existing_fields = {getattr(f, "name", str(f)) for f in lance_ds.schema}
    except Exception:
        names = []
        try:
            names = list(getattr(lance_ds.schema, "names", []))
        except Exception:
            try:
                names = [getattr(f, "name", str(f)) for f in getattr(lance_ds.schema, "fields", [])]
            except Exception:
                names = []
        existing_fields = set(names)

    if read_columns is None:
        update_cols: list[str] = []
        for c in df.column_names:
            if c in ("fragment_id", join_key):
                continue
            if c in meta_columns:
                raise ValueError(
                    f"Cannot update metadata column '{c}' via update_columns_from_df; remove it from the DataFrame."
                )
            if c not in existing_fields:
                raise ValueError(
                    f"Column '{c}' does not exist in the target dataset; "
                    f"update_columns_from_df only updates existing columns."
                )
            update_cols.append(c)

        if len(update_cols) == 0:
            raise ValueError(
                "No columns to update; DataFrame must contain at least one existing dataset column "
                "besides join key and 'fragment_id'."
            )

        read_columns = [join_key] + update_cols
    else:
        if join_key not in read_columns:
            raise ValueError(f"read_columns must include the join key '{join_key}'.")

        # Validate columns in read_columns
        for c in read_columns:
            if c not in df.column_names:
                raise ValueError(f"Column '{c}' specified in read_columns is not present in the DataFrame.")

        for c in read_columns:
            if c == join_key:
                continue
            if c in meta_columns:
                raise ValueError(
                    f"Cannot update metadata column '{c}' via update_columns_from_df; remove it from read_columns."
                )
            if c not in existing_fields:
                raise ValueError(
                    f"Column '{c}' does not exist in the target dataset; "
                    f"update_columns_from_df only updates existing columns."
                )

    # Ensure we open the same version of the dataset on workers, unless overridden by open_kwargs
    udf_open_kwargs = {"version": lance_ds.version, **(open_kwargs or {})}

    handler_udf = GroupFragmentUpdateUDF.with_init_args(  # type: ignore[attr-defined]
        str(uri),
        udf_open_kwargs,
        effective_left_on,
        join_key,
        read_columns,
        batch_size,
    )

    # Use map_groups for per-fragment processing.
    grouped = df.groupby("fragment_id").map_groups(
        handler_udf(*(df[c] for c in read_columns), df["fragment_id"]).alias("commit_message")
    )

    commit_messages = grouped.collect().to_pydict()["commit_message"]

    updated_fragments = []
    all_fields_modified: set[int] = set()

    for commit_message in commit_messages:
        fragment_meta_bytes = commit_message["fragment_meta"]
        fields_modified_bytes = commit_message["fields_modified"]

        if fragment_meta_bytes is None or fields_modified_bytes is None:
            continue

        fragment_meta = daft.pickle.loads(fragment_meta_bytes)
        fields_modified = daft.pickle.loads(fields_modified_bytes)
        updated_fragments.append(fragment_meta)
        for fid in fields_modified:
            all_fields_modified.add(int(fid))

    if not updated_fragments:
        # Nothing to update; return original dataset handle.
        return lance_ds

    op = lance.LanceOperation.Update(
        updated_fragments=updated_fragments,
        fields_modified=sorted(all_fields_modified),
    )

    commit_storage_options = (open_kwargs or {}).get("storage_options")

    return lance_ds.commit(
        uri,
        op,
        read_version=lance_ds.version,
        storage_options=commit_storage_options,
    )
