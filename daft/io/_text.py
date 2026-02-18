# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.file import File
from daft.functions import file as file_expr
from daft.io.file_path import from_glob_path
from daft.udf import func


@func
def _read_text_file(file: File, encoding: str, drop_empty_lines: bool) -> Iterator[str]:
    """Read a file as text and yield one element per line.

    Args:
        file: File object to read from.
        encoding: Text encoding used to decode the file contents.
        drop_empty_lines: If True, skip empty lines. If False, keep them.
    """
    with file.open() as f:
        data = f.read()

    if not data:
        return

    text = data.decode(encoding)
    for line in text.splitlines():
        if drop_empty_lines and line == "":
            continue
        yield line


@PublicAPI
def read_text(
    path: str | list[str],
    encoding: str = "utf-8",
    drop_empty_lines: bool = True,
    io_config: IOConfig | None = None,
    include_paths: bool = False,
) -> DataFrame:
    """Creates a DataFrame from text file(s).

    Each line in the input files becomes a row in the output DataFrame.

    Args:
        path (str | list[str]):
            Path to text files. Supports globs and remote URLs such as ``s3://`` or ``gs://``.
        encoding (str):
            Encoding used to decode file contents. Defaults to ``"utf-8"``.
        drop_empty_lines (bool):
            Whether to drop empty lines from the output. Defaults to True.
        io_config (IOConfig, optional):
            Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.
            If not provided, the default context configuration is used.
        include_paths (bool):
            If True, include a ``"path"`` column with the source file path for each line. Defaults to False.

    Returns:
        DataFrame: A DataFrame with one row per line of text.
            The output schema is:

            * ``text`` (String): line contents
            * ``path`` (String, optional): source file path when ``include_paths=True``

    Examples:
        Read a text file from a local path:

        >>> df = daft.read_text("/path/to/file.txt")
        >>> df = daft.read_text("/path/to/files-*.txt")

        Read text files from a public S3 bucket:

        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_text("s3://path/to/files-*.txt", io_config=io_config)
        >>> df.show()
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of text filepaths")

    daft_context = get_context()
    io_config = daft_context.daft_planning_config.default_io_config if io_config is None else io_config

    files_df = from_glob_path(path, io_config=io_config)

    # Create a File expression for each path so we can read contents via the native I/O layer.
    files_df = files_df.select(
        files_df["path"],
        file_expr(files_df["path"], io_config=io_config).alias("_file"),
    )

    # Apply the generator UDF to produce one row per line of text.
    lines_df = files_df.select(
        files_df["path"],
        _read_text_file(files_df["_file"], encoding, drop_empty_lines).alias("text"),
    )

    if include_paths:
        return lines_df.select("text", "path")

    return lines_df.select("text")
