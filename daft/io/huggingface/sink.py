from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING

from huggingface_hub.hf_api import HfApi

from daft.datatype import DataType
from daft.dependencies import pq
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema
from daft.utils import get_arrow_version

if TYPE_CHECKING:
    from collections.abc import Iterator

    from huggingface_hub import CommitOperationAdd, HfApi

    from daft.daft import HuggingFaceConfig


@dataclass
class CommitOperationAddWrapper:
    """Wrapper class to prevent the need for `CommitOperationAdd` to be imported outside of the type checking block.

    It needs to be available outside of the type checking block because it's used as a generic parameter and thus requires the type to be available at runtime.

    More details here: https://github.com/astral-sh/ruff/issues/7214
    """

    inner: CommitOperationAdd


class HuggingFaceSink(DataSink[CommitOperationAddWrapper]):
    def __init__(
        self,
        repo: str,
        split: str,
        data_dir: str,
        revision: str,
        overwrite: bool,
        commit_message: str,
        commit_description: str | None,
        config: HuggingFaceConfig,
    ):
        self.repo = repo
        self.split = split
        self.data_dir = data_dir
        self.revision = revision
        self.overwrite = overwrite
        self.commit_message = commit_message
        self.commit_description = commit_description
        self.config = config

        from importlib.util import find_spec

        if find_spec("huggingface_hub") is None:
            raise ImportError(
                "huggingface-hub must be installed to run `write_huggingface`. Install it with `pip install -U 'daft[huggingface]'`"
            )

        if self.config.anonymous:
            raise ValueError("Cannot write to Hugging Face dataset in anonymous mode.")

        self.path_prefix = f"{self.data_dir}/{self.split}".strip("/")

        if get_arrow_version() < (21, 0, 0):
            if self.config.use_content_defined_chunking:
                import warnings

                warnings.warn(
                    "HuggingFaceConfig.use_content_defined_chunking is enabled but requires PyArrow >=21.0.0. This setting will be ignored."
                )

            self.writer_kwargs = {}
        else:
            self.writer_kwargs = {
                "use_content_defined_chunking": bool(self.config.use_content_defined_chunking),
            }

    def name(self) -> str:
        return "Hugging Face Dataset Write"

    @cache
    def schema(self) -> Schema:
        return Schema.from_pydict(
            {
                "path_in_repo": DataType.string(),
                "operation": DataType.string(),
                "src_path_in_repo": DataType.string(),
            }
        )

    @contextmanager
    def _hf_api(self) -> Iterator[HfApi]:
        """Context manager to get an authenticated Hugging Face API client with progress bars disabled."""
        from huggingface_hub import HfApi
        from huggingface_hub.utils import disable_progress_bars, enable_progress_bars

        api = HfApi(token=self.config.token, library_name="daft")
        disable_progress_bars()
        try:
            yield api
        finally:
            enable_progress_bars()

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[CommitOperationAddWrapper]]:
        import io
        import uuid

        from huggingface_hub import CommitOperationAdd

        id = uuid.uuid4()
        num_files = 0
        num_rows = 0

        with self._hf_api() as api:
            with io.BytesIO() as buffer:
                writer = None

                def flush(writer: pq.ParquetWriter) -> WriteResult[CommitOperationAddWrapper]:
                    writer.close()

                    nonlocal num_files
                    nonlocal num_rows

                    path_in_repo = f"{self.path_prefix}-{id}-{num_files}.parquet"
                    num_files += 1

                    addition = CommitOperationAdd(path_in_repo=path_in_repo, path_or_fileobj=buffer.getvalue())
                    api.preupload_lfs_files(
                        repo_id=self.repo, additions=[addition], repo_type="dataset", revision=self.revision
                    )

                    # Reuse the buffer for the next file
                    buffer.seek(0)
                    buffer.truncate()

                    result = WriteResult(
                        CommitOperationAddWrapper(addition),
                        bytes_written=addition.upload_info.size,
                        rows_written=num_rows,
                    )

                    num_rows = 0

                    return result

                for part in micropartitions:
                    if writer is None:
                        writer = pq.ParquetWriter(buffer, part.schema().to_pyarrow_schema(), **self.writer_kwargs)

                    num_rows += len(part)
                    writer.write_table(part.to_arrow(), row_group_size=self.config.row_group_size)

                    if buffer.tell() > self.config.target_filesize:
                        yield flush(writer)

                        # delete the writer and create one for the next partition
                        writer = None

                # flush remaining data
                if writer is not None:
                    yield flush(writer)

    def finalize(self, write_results: list[WriteResult[CommitOperationAddWrapper]]) -> MicroPartition:
        from huggingface_hub import CommitOperation, CommitOperationAdd, CommitOperationCopy, CommitOperationDelete
        from huggingface_hub.hf_api import RepoFile, RepoFolder

        additions = [res.result.inner for res in write_results]
        operations = {}
        count_new = len(additions)
        count_existing = 0
        output: dict[str, list[str]] = {"path_in_repo": [], "operation": [], "src_path_in_repo": []}

        def list_split(api: HfApi) -> Iterator[RepoFile | RepoFolder]:
            """Get all existing files of the current split."""
            from huggingface_hub.utils import EntryNotFoundError

            try:
                objects = api.list_repo_tree(
                    path_in_repo=self.data_dir,
                    repo_id=self.repo,
                    repo_type="dataset",
                    revision=self.revision,
                    expand=False,
                    recursive=False,
                )
                for obj in objects:
                    if obj.path.startswith(self.path_prefix):
                        yield obj
            except EntryNotFoundError:
                pass

        def create_commits(api: HfApi, operations: list[CommitOperation], message: str) -> None:
            """Split the commit into multiple parts if necessary.

            The HuggingFace API may time out if there are too many operations in a single commit.
            """
            import math

            max_ops = self.config.max_operations_per_commit
            num_commits = math.ceil(len(operations) / max_ops)
            for i in range(num_commits):
                begin = i * max_ops
                end = (i + 1) * max_ops
                part = operations[begin:end]
                commit_message = message + (f" (part {i:05d}-of-{num_commits:05d})" if num_commits > 1 else "")
                api.create_commit(
                    repo_id=self.repo,
                    repo_type="dataset",
                    revision=self.revision,
                    operations=part,
                    commit_message=commit_message,
                    commit_description=self.commit_description,
                )

            for op in operations:
                if isinstance(op, CommitOperationAdd):
                    output["path_in_repo"].append(op.path_in_repo)
                    output["operation"].append("ADD")
                    output["src_path_in_repo"].append("")
                elif isinstance(op, CommitOperationCopy):
                    output["path_in_repo"].append(op.path_in_repo)
                    output["operation"].append("COPY")
                    output["src_path_in_repo"].append(op.src_path_in_repo)
                elif isinstance(op, CommitOperationDelete):
                    output["path_in_repo"].append(op.path_in_repo)
                    output["operation"].append("DELETE")
                    output["src_path_in_repo"].append("")
                else:
                    raise ValueError(f"Expected `operations` to be a list of `CommitOperation` type, found: {type(op)}")

        def format_path(i: int) -> str:
            return f"{self.path_prefix}-{i:05d}-of-{count_new + count_existing:05d}.parquet"

        def rename(old_path: str, new_path: str) -> Iterator[CommitOperation]:
            if old_path != new_path:
                yield CommitOperationCopy(src_path_in_repo=old_path, path_in_repo=new_path)
                yield CommitOperationDelete(path_in_repo=old_path)

        with self._hf_api() as api:
            # In overwrite mode, delete existing files
            if self.overwrite:
                for obj in list_split(api):
                    # Delete old file
                    operations[obj.path] = CommitOperationDelete(
                        path_in_repo=obj.path, is_folder=isinstance(obj, RepoFolder)
                    )
            # In append mode, rename existing files to have the correct total number of parts
            else:
                rename_operations: list[CommitOperation] = []
                existing = list(obj for obj in list_split(api) if isinstance(obj, RepoFile))
                count_existing = len(existing)
                for i, obj in enumerate(existing):
                    new_path = format_path(i)
                    rename_operations.extend(rename(obj.path, new_path))
                # Rename files in a separate commit to prevent them from being overwritten by new files of the same name
                create_commits(
                    api,
                    operations=rename_operations,
                    message=f"{self.commit_message} (rename existing files before uploading new files)",
                )

            # Rename additions, putting them after existing files if any
            for i, addition in enumerate(additions):
                addition.path_in_repo = format_path(i + count_existing)
                # Overwrite the deletion operation if the file already exists
                operations[addition.path_in_repo] = addition

            # Upload the new files
            create_commits(api, operations=list(operations.values()), message=self.commit_message)

        return MicroPartition.from_pydict(output)
