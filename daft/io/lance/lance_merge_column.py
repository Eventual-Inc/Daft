from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import daft.pickle
from daft.datatype import DataType
from daft.udf import udf

if TYPE_CHECKING:
    import lance

    from daft.dependencies import pa


@udf(return_dtype=DataType.struct({"fragment_meta": DataType.binary(), "schema": DataType.binary()}), concurrency=1)
class FragmentHandler:
    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        transform: dict[str, str] | lance.udf.BatchUDF | Callable[[pa.lib.RecordBatch], pa.lib.RecordBatch],
        read_columns: list[str] | None,
        reader_schema: pa.Schema | None = None,
    ):
        self.lance_ds = lance_ds
        self.transform = transform
        self.read_columns = read_columns
        self.reader_schema = reader_schema

    def __call__(self, fragment_ids: list[int]) -> list[dict[str, bytes]]:
        results = []
        for fragment_id in fragment_ids:
            fragment = self.lance_ds.get_fragment(fragment_id)
            fragment_meta, schema = fragment.merge_columns(self.transform, self.read_columns, None, self.reader_schema)
            results.append({"fragment_meta": daft.pickle.dumps(fragment_meta), "schema": daft.pickle.dumps(schema)})
        return results


def merge_columns_internal(
    lance_ds: lance.LanceDataset,
    uri: str,
    *,
    transform: dict[str, str] | lance.udf.BatchUDF | Callable[[pa.RecordBatch], pa.RecordBatch],
    read_columns: list[str] | None = None,
    reader_schema: pa.Schema | None = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
) -> None:
    import lance

    from daft import from_pylist
    from daft.udf.legacy import _UnsetMarker

    # Handle None case for daft_remote_args
    if daft_remote_args is None:
        daft_remote_args = {}

    num_cpus = daft_remote_args.get("num_cpus", _UnsetMarker)
    num_gpus = daft_remote_args.get("num_gpus", _UnsetMarker)
    memory_bytes = daft_remote_args.get("memory_bytes", _UnsetMarker)
    batch_size = daft_remote_args.get("batch_size", _UnsetMarker)
    fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
    fragment_data = [{"fragment_id": fid} for fid in fragment_ids]

    df = from_pylist(fragment_data)
    handler_fragment_udf = (
        FragmentHandler.with_init_args(lance_ds, transform, read_columns, reader_schema)  # type: ignore[attr-defined]
        .override_options(num_cpus=num_cpus, num_gpus=num_gpus, memory_bytes=memory_bytes, batch_size=batch_size)
        .with_concurrency(concurrency)
    )
    df = df.with_column("commit_message", handler_fragment_udf(df["fragment_id"]))

    commit_messages = df.collect().to_pydict()["commit_message"]
    new_schema = None
    fragment_metas = []
    for commit_message in commit_messages:
        fragment_meta = commit_message["fragment_meta"]
        schema = commit_message["schema"]
        fragment_metas.append(daft.pickle.loads(fragment_meta))
        if new_schema is None:
            new_schema = daft.pickle.loads(schema)
            continue
    if new_schema is None:
        raise ValueError("No schema for new fragment found")
    op = lance.LanceOperation.Merge(fragment_metas, new_schema)
    lance_ds.commit(
        uri,
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
    )
