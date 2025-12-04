"""URL Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft import context
from daft.expressions import Expression
from daft.runners import get_or_create_runner

if TYPE_CHECKING:
    from daft.daft import IOConfig


def _should_use_multithreading_tokio_runtime() -> bool:
    """Whether or not our expression should use the multithreaded tokio runtime under the hood, or a singlethreaded one.

    This matters because for distributed workloads, each process has its own tokio I/O runtime. if each distributed process
    is multithreaded (by default we spin up `N_CPU` threads) then we will be running `(N_CPU * N_PROC)` number of threads, and
    opening `(N_CPU * N_PROC * max_connections)` number of connections. This is too large for big machines with many CPU cores.

    Hence for Ray we default to doing the singlethreaded runtime. This means that we will have a limit of
    `(singlethreaded=1 * N_PROC * max_connections)` number of open connections per machine, which works out to be reasonable at ~2-4k connections.

    For local execution, we run in a single process which means that it all shares the same tokio I/O runtime and connection pool.
    Thus we just have `(multithreaded=N_CPU * max_connections)` number of open connections, which is usually reasonable as well.
    """
    using_ray_runner = get_or_create_runner().name == "ray"
    return not using_ray_runner


def _override_io_config_max_connections(max_connections: int, io_config: IOConfig | None) -> IOConfig:
    """Use a user-provided `max_connections` argument to override the value in S3Config.

    This is because our Rust code under the hood actually does `min(S3Config's max_connections, url_download's max_connections)` to
    determine how many connections to allow per-thread. Thus we need to override the io_config here to ensure that the user's max_connections
    is correctly applied in our Rust code.
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    io_config = io_config.replace(s3=io_config.s3.replace(max_connections=max_connections))
    return io_config


def download(
    expr: Expression,
    max_connections: int = 32,
    on_error: Literal["raise", "null"] = "raise",
    io_config: IOConfig | None = None,
) -> Expression:
    """Treats each string as a URL, and downloads the bytes contents as a bytes column.

    Args:
        expr: The expression to download.
        max_connections: The maximum number of connections to use per thread to use for downloading URLs. Defaults to 32.
        on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
            the error but fallback to a Null value. Defaults to "raise".
        io_config: IOConfig to use when accessing remote storage. Note that the S3Config's `max_connections` parameter will be overridden
            with `max_connections` that is passed in as a kwarg.

    Returns:
        Expression: a Binary expression which is the bytes contents of the URL, or None if an error occurred during download

    Note:
        If you are observing excessive S3 issues (such as timeouts, DNS errors or slowdown errors) during URL downloads,
        you may wish to reduce the value of ``max_connections`` (defaults to 32) to reduce the amount of load you are placing
        on your S3 servers.

        Alternatively, if you are running on machines with lower number of cores but very high network bandwidth, you can increase
        ``max_connections`` to get higher throughput with additional parallelism
    """
    multi_thread = _should_use_multithreading_tokio_runtime()
    io_config = _override_io_config_max_connections(max_connections, io_config)

    if io_config.unity.endpoint is None:
        try:
            from daft.catalog.__unity import UnityCatalog
        except ImportError:
            pass
        else:
            from daft.session import current_catalog

            catalog = current_catalog()
            if isinstance(catalog, UnityCatalog):
                unity_catalog = catalog._inner
                io_config = io_config.replace(unity=unity_catalog.to_io_config().unity)

    return Expression._call_builtin_scalar_fn(
        "url_download",
        expr,
        multi_thread=multi_thread,
        on_error=on_error,
        max_connections=max_connections,
        io_config=io_config,
    )


def upload(
    expr: Expression,
    location: str | Expression,
    max_connections: int = 32,
    on_error: Literal["raise", "null"] = "raise",
    io_config: IOConfig | None = None,
) -> Expression:
    """Uploads a column of binary data to the provided location(s) (also supports S3, local etc).

    Files will be written into the location (folder(s)) with a generated UUID filename, and the result
    will be returned as a column of string paths that is compatible with the ``download()`` Expression.

    Args:
        expr: The expression to upload.
        location: a folder location or column of folder locations to upload data into
        max_connections: The maximum number of connections to use per thread to use for uploading data. Defaults to 32.
        on_error: Behavior when a URL upload error is encountered - "raise" to raise the error immediately or "null" to log
            the error but fallback to a Null value. Defaults to "raise".
        io_config: IOConfig to use when uploading data

    Returns:
        Expression: a String expression containing the written filepath

    Examples:
        >>> from daft.functions import upload
        >>>
        >>> upload(df["data"], "s3://my-bucket/my-folder")  # doctest: +SKIP

        Upload to row-specific URLs

        >>> upload(df["data"], df["paths"])  # doctest: +SKIP

    """
    multi_thread = _should_use_multithreading_tokio_runtime()
    # If the user specifies a single location via a string, we should upload to a single folder. Otherwise,
    # if the user gave an expression, we assume that each row has a specific url to upload to.
    # Consider moving the check for is_single_folder to a lower IR.
    is_single_folder = isinstance(location, str)
    io_config = _override_io_config_max_connections(max_connections, io_config)

    return Expression._call_builtin_scalar_fn(
        "url_upload",
        expr,
        location,
        max_connections=max_connections,
        on_error=on_error,
        multi_thread=multi_thread,
        is_single_folder=is_single_folder,
        io_config=io_config,
    )


def parse_url(expr: Expression) -> Expression:
    """Parse string URLs and extract URL components.

    Returns:
        Expression: a Struct expression containing the parsed URL components:
            - scheme (str): The URL scheme (e.g., "https", "http")
            - username (str): The username, if present
            - password (str): The password, if present
            - host (str): The hostname or IP address
            - port (int): The port number, if specified
            - path (str): The path component
            - query (str): The query string, if present
            - fragment (str): The fragment/anchor, if present

    Examples:
        >>> import daft
        >>> from daft.functions import parse_url
        >>>
        >>> df = daft.from_pydict(
        ...     {"urls": ["https://user:pass@example.com:8080/path?query=value#fragment", "http://localhost/api"]}
        ... )
        >>> # Parse URLs and expand all components
        >>> df.select(parse_url(df["urls"]).unnest()).collect()  # doctest: +SKIP

    Note:
        Invalid URLs will result in null values for all components.
        The parsed result is automatically aliased to 'urls' to enable easy struct field expansion.
    """
    return Expression._call_builtin_scalar_fn("url_parse", expr)
