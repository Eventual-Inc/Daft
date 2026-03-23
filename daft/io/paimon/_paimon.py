# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.io import IOConfig, S3Config
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pypaimon.table.table import Table as PaimonTable


def _convert_paimon_catalog_options_to_io_config(catalog_options: dict) -> IOConfig | None:
    """Convert pypaimon catalog options to Daft IOConfig.

    pypaimon supports only S3-like (s3://, s3a://, s3n://, oss://), HDFS, and local (file://).

    OSS uses Daft's OpenDAL backend (opendal_backends={"oss": {...}}) rather than S3Config,
    because Daft routes oss:// URIs to OpenDAL's OSS operator, not the S3-compatible path.
    """
    from urllib.parse import urlparse

    warehouse = catalog_options.get("warehouse", "")
    scheme = urlparse(warehouse).scheme if warehouse else ""

    if scheme == "oss":
        # OSS requires OpenDAL backend configuration
        parsed = urlparse(warehouse)
        oss_cfg: dict[str, str] = {}

        bucket = parsed.netloc
        if bucket:
            oss_cfg["bucket"] = bucket

        endpoint = catalog_options.get("fs.oss.endpoint")
        if endpoint:
            if not endpoint.startswith(("http://", "https://")):
                endpoint = f"https://{endpoint}"
            oss_cfg["endpoint"] = endpoint

        key_id = catalog_options.get("fs.oss.accessKeyId")
        if key_id:
            oss_cfg["access_key_id"] = key_id

        key_secret = catalog_options.get("fs.oss.accessKeySecret")
        if key_secret:
            oss_cfg["access_key_secret"] = key_secret

        region = catalog_options.get("fs.oss.region")
        if region:
            oss_cfg["region"] = region

        token = catalog_options.get("fs.oss.securityToken")
        if token:
            oss_cfg["security_token"] = token

        return IOConfig(opendal_backends={"oss": oss_cfg}) if oss_cfg else None

    # S3-compatible (s3://, s3a://, s3n://)
    any_props_set = False

    def get(key: str):
        nonlocal any_props_set
        val = catalog_options.get(key)
        if val is not None:
            any_props_set = True
        return val

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=get("fs.s3.endpoint"),
            region_name=get("fs.s3.region"),
            key_id=get("fs.s3.accessKeyId"),
            access_key=get("fs.s3.accessKeySecret"),
            session_token=get("fs.s3.securityToken"),
        ),
    )

    return io_config if any_props_set else None


@PublicAPI
def read_paimon(
    table: "PaimonTable",
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Create a DataFrame from an Apache Paimon table.

    Args:
        table (pypaimon.table.Table): A Paimon table object created using the pypaimon library.
            Use ``pypaimon.CatalogFactory.create(options).get_table(identifier)`` to obtain one.
        io_config (IOConfig, optional): A custom IOConfig to use when accessing Paimon object
            storage data. If provided, any credentials in the catalog options are ignored.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Paimon table.

    Note:
        This function requires the use of `pypaimon <https://pypi.org/project/pypaimon/>`_,
        the Apache Paimon official Python API.

        For primary-key tables that require LSM-tree merge (i.e. splits with multiple files
        at overlapping levels), reads fall back to pypaimon's native reader. Append-only tables
        and single-file PK splits are read via Daft's native high-performance Parquet reader.

    Examples:
        Read an append-only Paimon table from a local warehouse:

        >>> import pypaimon
        >>> catalog = pypaimon.CatalogFactory.create({"warehouse": "/path/to/warehouse"})
        >>> table = catalog.get_table("mydb.mytable")
        >>> df = daft.read_paimon(table)
        >>> df.show()

        Read a table from an OSS-backed warehouse with custom IO config:

        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(endpoint_url="http://oss-cn-hangzhou.aliyuncs.com"))
        >>> catalog = pypaimon.CatalogFactory.create({
        ...     "warehouse": "oss://my-bucket/warehouse",
        ...     "fs.oss.accessKeyId": "...",
        ...     "fs.oss.accessKeySecret": "...",
        ... })
        >>> table = catalog.get_table("mydb.mytable")
        >>> df = daft.read_paimon(table, io_config=io_config)
        >>> df.show()
    """
    try:
        import pypaimon  # noqa: F401
    except ImportError:
        raise ImportError(
            "pypaimon is required to use read_paimon. "
            "Install it with: `pip install pypaimon`"
        )

    from daft.io.paimon.paimon_scan import PaimonScanOperator

    file_io = getattr(table, "file_io", None)
    catalog_options = (getattr(file_io, "properties", None) or {}) if file_io is not None else {}

    # Infer IO config: user-provided > catalog options > default
    io_config = io_config or _convert_paimon_catalog_options_to_io_config(catalog_options)
    io_config = io_config or context.get_context().daft_planning_config.default_io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    warehouse = catalog_options.get("warehouse", "")
    scan_catalog_options = {"warehouse": warehouse} if warehouse else {}

    operator = PaimonScanOperator(table, storage_config=storage_config, catalog_options=scan_catalog_options)
    handle = ScanOperatorHandle.from_python_scan_operator(operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
