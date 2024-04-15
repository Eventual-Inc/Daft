from __future__ import annotations

from typing_extensions import Literal

from daft import Expression, context
from daft.daft import IOConfig
from daft.expressions.expressions import ExpressionNamespace


class ExpressionUrlNamespace(ExpressionNamespace):
    def download(
        self,
        max_connections: int = 32,
        on_error: Literal["raise"] | Literal["null"] = "raise",
        io_config: IOConfig | None = None,
        use_native_downloader: bool = True,
    ) -> Expression:
        """Treats each string as a URL, and downloads the bytes contents as a bytes column

        .. NOTE::
            If you are observing excessive S3 issues (such as timeouts, DNS errors or slowdown errors) during URL downloads,
            you may wish to reduce the value of ``max_connections`` (defaults to 32) to reduce the amount of load you are placing
            on your S3 servers.

            Alternatively, if you are running on machines with lower number of cores but very high network bandwidth, you can increase
            ``max_connections`` to get higher throughput with additional parallelism

        Args:
            max_connections: The maximum number of connections to use per thread to use for downloading URLs. Defaults to 32.
            on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
                the error but fallback to a Null value. Defaults to "raise".
            io_config: IOConfig to use when accessing remote storage. Note that the S3Config's `max_connections` parameter will be overridden
                with `max_connections` that is passed in as a kwarg.
            use_native_downloader (bool): Use the native downloader rather than python based one.
                Defaults to True.

        Returns:
            Expression: a Binary expression which is the bytes contents of the URL, or None if an error occured during download
        """
        if use_native_downloader:
            raise_on_error = False
            if on_error == "raise":
                raise_on_error = True
            elif on_error == "null":
                raise_on_error = False
            else:
                raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")

            if not (isinstance(max_connections, int) and max_connections > 0):
                raise ValueError(f"Invalid value for `max_connections`: {max_connections}")

            # Use the `max_connections` kwarg to override the value in S3Config
            # This is because the max parallelism is actually `min(S3Config's max_connections, url_download's max_connections)` under the hood.
            # However, default max_connections on S3Config is only 8, and even if we specify 32 here we are bottlenecked there.
            # Therefore for S3 downloads, we override `max_connections` kwarg to have the intended effect.
            io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
            io_config = io_config.replace(s3=io_config.s3.replace(max_connections=max_connections))

            using_ray_runner = context.get_context().is_ray_runner
            return Expression._from_pyexpr(
                self._expr.url_download(max_connections, raise_on_error, not using_ray_runner, io_config)
            )
        else:
            from daft.udf_library import url_udfs

            return url_udfs.download_udf(
                Expression._from_pyexpr(self._expr),
                max_worker_threads=max_connections,
                on_error=on_error,
            )
