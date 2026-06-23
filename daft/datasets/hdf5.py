"""Generic HDF5 readers for `daft.datasets`.

An HDF5 file is like a filesystem inside a single file: it contains *groups*
(folders), *datasets* (n-dimensional arrays, each with a numpy dtype and a
shape), and *attributes* (small key-value tags attached to groups/datasets). A
very common layout -- used by e.g. EgoDex (https://github.com/apple/ml-egodex)
-- is "one file per episode", where most datasets share a leading time
dimension ``N`` (one entry per frame) plus a few constant datasets that don't:

    camera/intrinsic              (3, 3)       constant per file
    transforms/leftHand           (N, 4, 4)    one 4x4 pose per frame
    confidences/leftHand          (N,)         one scalar per frame

and per-file root attributes (e.g. EgoDex's ``llm_description`` task text).

:func:`read` maps that layout onto a DataFrame with one row per frame:
datasets whose leading dimension matches the file's "row axis" contribute one
element per row, everything else is broadcast (repeated) onto every row, and
requested ``attrs`` are broadcast as additional columns.
:func:`read_datasets` is the exploration view -- one row per (file, dataset)
with its name/shape/dtype -- useful for deciding what to pass to :func:`read`.
"""

from __future__ import annotations

import io
from collections import Counter
from typing import TYPE_CHECKING, Any, NamedTuple

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col
from daft.file import File
from daft.functions.file_ import file as file_expr
from daft.udf import func

if TYPE_CHECKING:
    import h5py
    import numpy as np

    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


# Files up to this size are read fully into memory before parsing. HDF5 reads
# metadata in many tiny seek+read calls, and each call on a Daft file handle
# crosses the Python<->Rust boundary individually (~14x slower than reading
# from RAM). Above the threshold we stream instead to bound memory usage.
_SLURP_MAX_BYTES = 256 * 1024 * 1024

# Schema of one read_datasets entry (per file, per dataset).
_DATASET_ENTRY_DTYPE = DataType.struct(
    {"name": DataType.string(), "shape": DataType.list(DataType.int64()), "dtype": DataType.string()}
)


class _ColumnSpec(NamedTuple):
    """How one HDF5 dataset becomes one DataFrame column.

    per_frame=True  -> the dataset's leading dim is the row axis; row i gets
                       element ``arr[i]``.
    per_frame=False -> the dataset is a per-file constant; every row gets the
                       whole array (broadcast).
    """

    name: str
    dtype: DataType
    per_frame: bool
    is_string: bool
    elem_shape: tuple[int, ...]
    np_dtype: str


class _AttrSpec(NamedTuple):
    """How one HDF5 root attribute becomes one (broadcast) DataFrame column.

    Attributes are per-file scalars/strings/small arrays, so each one is
    repeated onto every row of its file. ``kind`` is a small tag the loader
    maps to a pyarrow type (see ``_arrow_type_for_kind``).
    """

    name: str
    dtype: DataType
    kind: str


def _import_h5py() -> Any:
    try:
        import h5py
    except ImportError as err:
        raise ImportError("`daft.datasets.hdf5` requires h5py. Install with `pip install h5py`.") from err
    return h5py


def _open_h5(f: Any) -> h5py.File:
    """Open a file-like object as HDF5, preferring an in-memory copy for small files.

    h5py can read directly from any Python file object that supports
    read/seek/tell, but issues many small reads, which are slow against a Daft
    file handle. Small files are slurped into a BytesIO instead; large files
    are read in place, falling back to a slurp if h5py's driver rejects the
    handle.
    """
    h5py = _import_h5py()
    size = f.seek(0, io.SEEK_END)
    f.seek(0)
    if size <= _SLURP_MAX_BYTES:
        return h5py.File(io.BytesIO(f.read()), "r")
    try:
        return h5py.File(f, "r")
    except (OSError, ValueError, TypeError):
        f.seek(0)
        return h5py.File(io.BytesIO(f.read()), "r")


def _walk_datasets(h5: h5py.File) -> list[tuple[str, tuple[int, ...], np.dtype]]:
    """Collect (path, shape, dtype) for every dataset in the file.

    Groups are walked like directories; the leaves (datasets) are recorded by
    their full slash-separated path, e.g. ``transforms/leftHand``, in sorted
    order. Only metadata is read, never array data.
    """
    h5py = _import_h5py()
    found: list[tuple[str, tuple[int, ...], Any]] = []
    groups: list[Any] = [h5]
    while groups:
        group = groups.pop()
        for key in group:
            obj = group[key]
            if isinstance(obj, h5py.Dataset):
                # obj.name is the absolute path, e.g. "/transforms/leftHand"
                found.append((obj.name.lstrip("/"), tuple(obj.shape), obj.dtype))
            elif isinstance(obj, h5py.Group):
                groups.append(obj)
    found.sort(key=lambda entry: entry[0])
    return found


def _element_datatype(np_dtype: Any, elem_shape: tuple[int, ...]) -> DataType | None:
    """Map one dataset element (one row's worth of data) to a Daft DataType.

    Returns None for dtypes we can't represent (e.g. compound/struct dtypes),
    so callers can skip those datasets instead of failing.
    """
    h5py = _import_h5py()
    if h5py.check_string_dtype(np_dtype):
        # Variable-length strings only make sense as scalars-per-row here.
        return DataType.string() if elem_shape == () else None
    try:
        scalar = DataType.from_numpy_dtype(np_dtype)
    except Exception:
        return None
    if elem_shape == ():
        return scalar
    return DataType.tensor(scalar, shape=elem_shape)


def _normalize_attr(raw: Any) -> Any:
    """Convert a raw h5py attribute value to a plain Python value.

    h5py returns attributes as numpy scalars/arrays or bytes; we normalize to
    str / bool / int / float / list so they can go straight into a pyarrow
    column. Returns ``str(raw)`` for anything exotic.
    """
    import numpy as np

    if raw is None:
        return None
    if isinstance(raw, np.ndarray):
        if raw.ndim == 0:
            raw = raw.item()
        else:
            return [_normalize_attr(x) for x in raw.tolist()]
    if isinstance(raw, np.generic):
        raw = raw.item()
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    if isinstance(raw, (bool, int, float, str)):
        return raw
    return str(raw)


def _attr_kind_dtype(value: Any) -> tuple[str, DataType]:
    """Infer the (kind tag, Daft DataType) for a normalized attribute value."""
    # bool first: in Python `bool` is a subclass of `int`.
    if isinstance(value, bool):
        return "bool", DataType.bool()
    if isinstance(value, int):
        return "int", DataType.int64()
    if isinstance(value, float):
        return "float", DataType.float64()
    if isinstance(value, str):
        return "string", DataType.string()
    if isinstance(value, list):
        elem = next((x for x in value if x is not None), None)
        if isinstance(elem, bool):
            return "list_bool", DataType.list(DataType.bool())
        if isinstance(elem, int):
            return "list_int", DataType.list(DataType.int64())
        if isinstance(elem, float):
            return "list_float", DataType.list(DataType.float64())
        return "list_string", DataType.list(DataType.string())
    return "string", DataType.string()


def _arrow_type_for_kind(kind: str) -> Any:
    """Map an attribute ``kind`` tag to a pyarrow type (used inside the loader)."""
    import pyarrow as pa

    return {
        "bool": pa.bool_(),
        "int": pa.int64(),
        "float": pa.float64(),
        "string": pa.string(),
        "list_bool": pa.list_(pa.bool_()),
        "list_int": pa.list_(pa.int64()),
        "list_float": pa.list_(pa.float64()),
        "list_string": pa.list_(pa.string()),
    }[kind]


def _is_per_frame(name: str, shapes_per_file: list[dict[str, tuple[int, ...]]], row_dims: list[int]) -> bool:
    """A dataset is per-frame only if its leading dim matches the row axis in EVERY sampled file.

    This is what disambiguates a constant (3, 3) matrix from 3 frames of
    3-vectors: across two files with different N, only genuinely per-frame
    datasets keep tracking N. Files where the dataset is absent (HDF5 groups
    like EgoDex's confidences are optional) don't count against it.
    """
    for shapes, dim in zip(shapes_per_file, row_dims):
        shape = shapes.get(name)
        if shape is None:
            continue
        if len(shape) == 0 or shape[0] != dim:
            return False
    return True


def _load_files_batch(
    files: Any, specs: list[_ColumnSpec], attr_specs: list[_AttrSpec], index_column: str | None
) -> Any:
    """Read a batch of HDF5 files into one struct column, built directly in pyarrow.

    Each file's row count N comes from the first per-frame dataset present in
    it. The struct has one list field per dataset and per requested attribute:
    per-frame datasets contribute their N elements, broadcast datasets and
    attributes are repeated N times, datasets/attributes missing from a file
    are null-filled for its N rows, and ``index_column`` (if given) is a
    0..N-1 counter.

    Building the column with pyarrow instead of returning Python values is a
    deliberate performance choice: per-frame tensors would otherwise be
    converted to Arrow one tiny numpy object at a time (~70us each, dominating
    the read), whereas a flat float buffer wrapped as FixedSizeList is a bulk
    copy, and Daft's cast from FixedSizeList to FixedShapeTensor is zero-copy.
    """
    import numpy as np
    import pyarrow as pa

    h5_files = files.to_pylist()
    counts: list[int] = []
    per_file: dict[str, list[Any]] = {spec.name: [] for spec in specs}
    per_file_attr: dict[str, list[Any]] = {a.name: [] for a in attr_specs}
    for file in h5_files:
        with file.open() as f:
            with _open_h5(f) as h5:
                n = None
                for spec in specs:
                    if spec.per_frame and spec.name in h5:
                        n = h5[spec.name].shape[0]
                        break
                if n is None:
                    raise ValueError(f"No per-row datasets present in {file}; cannot determine its row count.")
                counts.append(n)
                for spec in specs:
                    if spec.name not in h5:
                        per_file[spec.name].append(None)
                        continue
                    ds = h5[spec.name]
                    if spec.per_frame and ds.shape[0] != n:
                        raise ValueError(
                            f"HDF5 dataset {spec.name!r} was inferred to be per-row, but in {file} it has "
                            f"leading dim {ds.shape[0]} while the file's row axis is {n}. "
                            f"If it is a per-file constant, pass broadcast=[{spec.name!r}]."
                        )
                    per_file[spec.name].append(ds.asstr()[...] if spec.is_string else ds[...])
                for attr in attr_specs:
                    per_file_attr[attr.name].append(
                        _normalize_attr(h5.attrs[attr.name]) if attr.name in h5.attrs else None
                    )

    offsets = pa.array(np.concatenate(([0], np.cumsum(counts, dtype=np.int64))), type=pa.int32())
    names: list[str] = []
    fields: list[Any] = []
    for spec in specs:
        chunks = per_file[spec.name]
        if spec.is_string:
            flat_strings: list[str | None] = []
            for n, chunk in zip(counts, chunks):
                if chunk is None:
                    flat_strings.extend([None] * n)
                elif spec.per_frame:
                    flat_strings.extend(chunk.tolist())
                else:
                    flat_strings.extend([str(chunk)] * n)
            elements = pa.array(flat_strings, type=pa.string())
        else:
            elem_size = 1
            for dim in spec.elem_shape:
                elem_size *= dim
            value_type = pa.from_numpy_dtype(np.dtype(spec.np_dtype))
            elem_type = value_type if spec.elem_shape == () else pa.list_(value_type, elem_size)
            parts: list[Any] = []
            for n, chunk in zip(counts, chunks):
                if chunk is None:
                    parts.append(pa.nulls(n, elem_type))
                    continue
                if spec.per_frame:
                    flat = chunk.reshape(n, -1)
                else:
                    flat = np.broadcast_to(chunk.reshape(1, -1), (n, elem_size))
                values = pa.array(np.ascontiguousarray(flat).reshape(-1))
                if values.type != value_type:
                    values = values.cast(value_type)
                parts.append(values if spec.elem_shape == () else pa.FixedSizeListArray.from_arrays(values, elem_size))
            elements = pa.concat_arrays(parts) if parts else pa.nulls(0, elem_type)
        names.append(spec.name)
        fields.append(pa.ListArray.from_arrays(offsets, elements))

    for attr in attr_specs:
        arrow_type = _arrow_type_for_kind(attr.kind)
        flat_attr: list[Any] = []
        for n, value in zip(counts, per_file_attr[attr.name]):
            flat_attr.extend([value] * n)
        elements = pa.array(flat_attr, type=arrow_type)
        names.append(attr.name)
        fields.append(pa.ListArray.from_arrays(offsets, elements))

    if index_column is not None:
        frame_indices = [np.arange(n, dtype=np.uint64) for n in counts]
        flat_index = np.concatenate(frame_indices) if frame_indices else np.empty(0, dtype=np.uint64)
        names.append(index_column)
        fields.append(pa.ListArray.from_arrays(offsets, pa.array(flat_index)))

    return pa.StructArray.from_arrays(fields, names)


@func(return_dtype=DataType.list(_DATASET_ENTRY_DTYPE))
def _list_file_datasets(file: File) -> list[dict[str, Any]]:
    """List the (name, shape, dtype) of every dataset in one HDF5 file."""
    with file.open() as f:
        with _open_h5(f) as h5:
            return [
                {"name": name, "shape": list(shape), "dtype": str(np_dtype)}
                for name, shape, np_dtype in _walk_datasets(h5)
            ]


def _glob_paths(path: str | list[str], io_config: IOConfig | None) -> DataFrame:
    return daft.from_glob_path(path, io_config=io_config).select("path")


def _head_paths(paths_df: DataFrame, path: str | list[str], n: int) -> list[str]:
    from daft.exceptions import DaftCoreException

    try:
        head = paths_df.limit(n).to_pydict()["path"]
    except DaftCoreException:
        # Daft's glob scan errors out when nothing matches; translate it into
        # the error a reader caller would expect.
        head = []
    if not head:
        raise FileNotFoundError(f"No files matched {path!r}")
    return head


@PublicAPI
def read(
    path: str | list[str],
    datasets: list[str] | None = None,
    broadcast: list[str] | None = None,
    attrs: list[str] | None = None,
    io_config: IOConfig | None = None,
    index_column: str | None = "row_index",
) -> DataFrame:
    """Read HDF5 files as a lazy DataFrame with one row per leading-dimension entry.

    The "row axis" is the most common leading dimension among a file's
    datasets (for episode-style data like EgoDex this is the number of frames
    N). Datasets that match it contribute one element per row; datasets that
    don't (e.g. a constant 3x3 ``camera/intrinsic``) are broadcast onto every
    row as whole arrays.

    The schema is inferred by sampling up to two matched files. Two, because a
    single file can be ambiguous: a constant ``(3, 3)`` matrix in a 3-frame
    file looks identical to per-frame 3-vectors. A second file with a
    different frame count resolves this -- per-frame datasets track N, constants
    don't. If every sampled file has the same N the ambiguity remains; use
    ``broadcast`` to pin such datasets explicitly.

    Args:
        path: A file path or glob, or a list of them (e.g.
            ``"part1/**/*.hdf5"``). Anything `daft.from_glob_path` accepts.
        datasets: Optional subset of dataset paths to read (e.g.
            ``["transforms/leftHand", "camera/intrinsic"]``). Defaults to every
            dataset with a representable dtype; datasets with unsupported
            dtypes (e.g. compound types) are skipped.
        broadcast: Dataset paths to always treat as per-file constants
            (repeated onto every row), overriding the row-axis inference.
        attrs: Names of HDF5 root attributes to surface as broadcast columns
            (e.g. ``["llm_description"]``). Each is repeated onto every row of
            its file; files missing the attribute get null. Strings, numeric
            scalars, booleans, and 1-D arrays of those are supported.
        io_config: Optional IO configuration for remote reads.
        index_column: Name for the per-file row counter column (the position
            along the row axis, i.e. the frame index). Pass None to omit it.

    Returns:
        Lazy DataFrame with columns ``path``, the index column, one column per
        dataset (named by its slash-separated HDF5 path), and one column per
        requested attribute.
    """
    paths_df = _glob_paths(path, io_config)
    sample_paths = _head_paths(paths_df, path, n=2)
    first = sample_paths[0]

    # Schema inference: read dataset metadata + root attributes (no array data)
    # from up to two files. "rb" = binary mode; HDF5 is a binary format, the
    # text-mode default would try to decode it as UTF-8 and fail.
    sampled: list[list[tuple[str, tuple[int, ...], Any]]] = []
    sampled_attrs: list[dict[str, Any]] = []
    for sample_path in sample_paths:
        with daft.open_file(sample_path, mode="rb", io_config=io_config) as f:
            with _open_h5(f) as h5:
                sampled.append(_walk_datasets(h5))
                sampled_attrs.append({k: _normalize_attr(v) for k, v in h5.attrs.items()})

    # Union the datasets across the sampled files (first occurrence wins for
    # shape/dtype): optional groups like EgoDex's confidences may be absent
    # from whichever file the glob happens to list first.
    seen: set[str] = set()
    found: list[tuple[str, tuple[int, ...], Any]] = []
    for file_found in sampled:
        for entry in file_found:
            if entry[0] not in seen:
                seen.add(entry[0])
                found.append(entry)
    found.sort(key=lambda entry: entry[0])
    if datasets is not None:
        missing = sorted(set(datasets) - {name for name, _, _ in found})
        if missing:
            raise ValueError(f"Datasets not found in {first!r}: {missing}")
        found = [entry for entry in found if entry[0] in set(datasets)]
    if not found:
        raise ValueError(f"No datasets to read in {first!r}")
    selected_names = {name for name, _, _ in found}

    forced_broadcast = set(broadcast or ())
    unknown_broadcast = sorted(forced_broadcast - selected_names)
    if unknown_broadcast:
        raise ValueError(f"`broadcast` names not found in {first!r}: {unknown_broadcast}")

    # Resolve requested attributes against the sampled files (first file that
    # has the attribute wins for type inference).
    attr_specs: list[_AttrSpec] = []
    for name in attrs or ():
        present = [sa[name] for sa in sampled_attrs if name in sa and sa[name] is not None]
        if present:
            kind, dtype = _attr_kind_dtype(present[0])
        else:
            # Absent (or null) in the sampled files; it may still appear in
            # others (datasets can be heterogeneous, e.g. EgoDex reset tasks
            # lack `which_llm_description`). Default to a nullable string column
            # rather than failing the read.
            kind, dtype = "string", DataType.string()
        attr_specs.append(_AttrSpec(name, dtype, kind))
    attr_names = {a.name for a in attr_specs}
    if attr_names & selected_names:
        raise ValueError(f"`attrs` names collide with dataset names: {sorted(attr_names & selected_names)}")

    # The row axis of each sampled file: the most common leading dimension
    # among its (selected) datasets. For EgoDex-style files this is N (frames);
    # the lone (3, 3) intrinsic loses the vote.
    shapes_per_file: list[dict[str, tuple[int, ...]]] = [
        {name: shape for name, shape, _ in file_found if name in selected_names} for file_found in sampled
    ]
    row_dims: list[int] = []
    for shapes in shapes_per_file:
        votes = Counter(shape[0] for shape in shapes.values() if len(shape) > 0)
        if not votes:
            raise ValueError(f"All requested datasets in {first!r} are scalars; nothing to use as a row axis.")
        row_dims.append(votes.most_common(1)[0][0])

    specs: list[_ColumnSpec] = []
    for name, shape, np_dtype in found:
        per_frame = name not in forced_broadcast and _is_per_frame(name, shapes_per_file, row_dims)
        elem_shape = shape[1:] if per_frame else shape
        dtype = _element_datatype(np_dtype, elem_shape)
        if dtype is None:
            continue
        specs.append(
            _ColumnSpec(name, dtype, per_frame, dtype == DataType.string(), elem_shape, str(np_dtype))
        )
    if not any(spec.per_frame for spec in specs):
        raise ValueError(f"No datasets in {first!r} share the inferred row axis (leading dim {row_dims[0]}).")

    # A UDF returns a single column, so pack one list field per dataset/attribute
    # into a struct, then unnest + explode so each frame becomes its own row. The
    # row counter rides along as its own list field. Field order must match the loader.
    struct_fields = {spec.name: DataType.list(spec.dtype) for spec in specs}
    for a in attr_specs:
        struct_fields[a.name] = DataType.list(a.dtype)
    explode_columns = [spec.name for spec in specs] + [a.name for a in attr_specs]
    if index_column is not None:
        if index_column in selected_names or index_column in attr_names:
            raise ValueError(f"index_column {index_column!r} collides with a dataset or attribute name.")
        struct_fields[index_column] = DataType.list(DataType.uint64())
        explode_columns.append(index_column)
    struct_dtype = DataType.struct(struct_fields)
    load_files = func.batch(return_dtype=struct_dtype)(_load_files_batch)

    # Small batches of file paths so the loader UDF runs in parallel across
    # files and downstream operators receive rows as soon as the first few
    # files are read, instead of waiting for the entire file list.
    df = paths_df.into_batches(16)
    df = df.with_column(
        "_data", load_files(file_expr(col("path"), io_config=io_config), specs, attr_specs, index_column)
    )
    df = df.select(col("path"), col("_data").unnest())
    df = df.explode(*explode_columns)
    return df


@PublicAPI
def read_datasets(path: str | list[str], io_config: IOConfig | None = None) -> DataFrame:
    """List every dataset in the matched HDF5 files, one row per (file, dataset).

    This is the exploration view: use it to see what's inside the files
    (names, shapes, dtypes) before choosing what to pass to :func:`read`.

    Args:
        path: A file path or glob, or a list of them. Anything
            `daft.from_glob_path` accepts.
        io_config: Optional IO configuration for remote reads.

    Returns:
        Lazy DataFrame with columns ``path``, ``name``, ``shape``, ``dtype``.
    """
    df = _glob_paths(path, io_config)
    df = df.into_batches(16)
    df = df.with_column("dataset", _list_file_datasets(file_expr(col("path"), io_config=io_config)))
    df = df.explode("dataset")
    df = df.select(col("path"), col("dataset").unnest())
    return df


__all__ = [
    "read",
    "read_datasets",
]
