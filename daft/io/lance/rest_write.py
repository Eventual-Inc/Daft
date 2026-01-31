# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from typing import Any

from daft.recordbatch import MicroPartition

from .rest_config import LanceRestConfig

# Make lance_namespace available for testing/mocking
try:
    import lance_namespace
except ImportError:
    lance_namespace = None

logger = logging.getLogger(__name__)


def write_lance_rest(
    mp: MicroPartition,
    rest_config: LanceRestConfig,
    namespace: str,
    table_name: str,
    mode: str = "append",
    schema: Any | None = None,
    **kwargs: Any,
) -> MicroPartition:
    """Write MicroPartition to Lance table via REST API.

    Args:
        mp: MicroPartition containing data to write
        rest_config: REST configuration for the Lance service
        namespace: Namespace containing the table
        table_name: Name of the table to write to
        mode: Write mode ("create", "append", "overwrite")
        schema: Optional schema for the table
        **kwargs: Additional arguments passed to the REST API

    Returns:
        MicroPartition containing write metadata
    """
    if lance_namespace is None:
        raise ImportError(
            "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
        )

    # Create REST client
    client_config: dict[str, Any] = {"uri": rest_config.base_url}
    if rest_config.api_key:
        client_config["api_key"] = rest_config.api_key
    if rest_config.headers:
        client_config["headers"] = rest_config.headers

    client = lance_namespace.connect("rest", client_config)

    # Convert MicroPartition to Arrow table
    arrow_table = mp.to_arrow()

    try:
        # Handle different write modes
        if mode == "create":
            # Create new table
            try:
                # Check if table exists
                table_exists = client.table_exists(namespace, table_name)
                if table_exists:
                    raise ValueError(
                        f"Table {namespace}/{table_name} already exists. Use mode='overwrite' or mode='append'"
                    )
            except Exception:
                # Assume table doesn't exist if check fails
                pass

            # Create the table with schema
            table_schema = schema if schema is not None else arrow_table.schema
            client.create_table(namespace=namespace, table_name=table_name, schema=table_schema, **kwargs)

        elif mode == "overwrite":
            # Drop and recreate table
            try:
                client.drop_table(namespace, table_name)
            except Exception:
                # Table might not exist, continue
                pass

            # Create new table
            table_schema = schema if schema is not None else arrow_table.schema
            client.create_table(namespace=namespace, table_name=table_name, schema=table_schema, **kwargs)

        # Insert data via REST API
        result = client.insert_records(
            namespace=namespace,
            table_name=table_name,
            data=arrow_table,
            mode="append",  # Always append after table creation/truncation
            **kwargs,
        )

        # Extract metadata from result
        transaction_id = getattr(result, "transaction_id", None) or "unknown"
        num_rows = len(arrow_table)

        # Return metadata as MicroPartition
        metadata = {
            "transaction_id": [transaction_id],
            "num_rows": [num_rows],
            "table_name": [table_name],
            "namespace": [namespace],
        }

        return MicroPartition.from_pydict(metadata)

    except Exception as e:
        logger.error("Failed to write to Lance REST table %s/%s: %s", namespace, table_name, e)
        raise


def create_lance_table_rest(
    rest_config: LanceRestConfig,
    namespace: str,
    table_name: str,
    schema: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Create a new Lance table via REST API.

    Args:
        rest_config: REST configuration for the Lance service
        namespace: Namespace to create the table in
        table_name: Name of the table to create
        schema: Arrow schema for the table
        **kwargs: Additional arguments passed to the REST API

    Returns:
        Dictionary containing creation metadata
    """
    if lance_namespace is None:
        raise ImportError(
            "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
        )

    # Create REST client
    client_config: dict[str, Any] = {"uri": rest_config.base_url}
    if rest_config.api_key:
        client_config["api_key"] = rest_config.api_key
    if rest_config.headers:
        client_config["headers"] = rest_config.headers

    client = lance_namespace.connect("rest", client_config)

    try:
        # Create the table
        result = client.create_table(namespace=namespace, table_name=table_name, schema=schema, **kwargs)

        return {"namespace": namespace, "table_name": table_name, "schema": schema, "result": result}

    except Exception as e:
        logger.error("Failed to create Lance REST table %s/%s: %s", namespace, table_name, e)
        raise


def register_lance_table_rest(
    rest_config: LanceRestConfig,
    namespace: str,
    table_name: str,
    table_uri: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Register an existing Lance table with the REST catalog.

    Args:
        rest_config: REST configuration for the Lance service
        namespace: Namespace to register the table in
        table_name: Name to register the table as
        table_uri: URI of the existing Lance table
        **kwargs: Additional arguments passed to the REST API

    Returns:
        Dictionary containing registration metadata
    """
    if lance_namespace is None:
        raise ImportError(
            "Unable to import the `lance_namespace` package, please ensure it is installed: `pip install lance-namespace`"
        )

    # Create REST client
    client_config: dict[str, Any] = {"uri": rest_config.base_url}
    if rest_config.api_key:
        client_config["api_key"] = rest_config.api_key
    if rest_config.headers:
        client_config["headers"] = rest_config.headers

    client = lance_namespace.connect("rest", client_config)

    try:
        # Register the table
        result = client.register_table(namespace=namespace, table_name=table_name, table_uri=table_uri, **kwargs)

        return {"namespace": namespace, "table_name": table_name, "table_uri": table_uri, "result": result}

    except Exception as e:
        logger.error("Failed to register Lance REST table %s/%s: %s", namespace, table_name, e)
        raise
