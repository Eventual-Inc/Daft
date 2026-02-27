#!/usr/bin/env python3
"""Example demonstrating Lance REST Namespace Catalog support in Daft.

This example shows how to read from and write to Lance tables via REST APIs,
which enables integration with catalog systems like LanceDB Cloud, Apache Gravitino,
and other services that implement the Lance REST Namespace specification.
"""

from __future__ import annotations

import daft
from daft.io.lance import LanceRestConfig


def main():
    print("Lance REST Namespace Catalog Example")
    print("=" * 40)

    # Configure REST connection
    rest_config = LanceRestConfig(
        base_url="https://api.lancedb.com",  # Replace with your Lance REST service URL
        api_key="your-api-key",  # Replace with your API key
        timeout=60,
        headers={"Custom-Header": "example-value"},  # Optional custom headers
    )

    print(f"REST Config: {rest_config}")

    # Create sample data
    sample_data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "score": [95.5, 87.2, 92.1, 88.9, 94.3],
        "active": [True, False, True, True, False],
    }

    df = daft.from_pydict(sample_data)
    print("\nSample DataFrame:")
    df.show()

    # Example 1: Write to REST endpoint (create mode)
    print("\n1. Writing to Lance table via REST (create mode)...")
    try:
        # Note: This will fail without a real REST service, but shows the API
        result = df.write_lance("rest://my_namespace/users_table", rest_config=rest_config, mode="create")
        print("Write successful!")
        result.show()
    except Exception as e:
        print(f"Expected error (no real REST service): {e}")

    # Example 2: Read from REST endpoint
    print("\n2. Reading from Lance table via REST...")
    try:
        # Note: This will fail without a real REST service, but shows the API
        df_read = daft.read_lance("rest://my_namespace/users_table", rest_config=rest_config)
        print("Read successful!")
        df_read.show()
    except Exception as e:
        print(f"Expected error (no real REST service): {e}")

    # Example 3: Append to existing table
    print("\n3. Appending to existing Lance table via REST...")
    new_data = {"id": [6, 7], "name": ["Frank", "Grace"], "score": [89.7, 91.4], "active": [True, True]}
    df_new = daft.from_pydict(new_data)

    try:
        result = df_new.write_lance("rest://my_namespace/users_table", rest_config=rest_config, mode="append")
        print("Append successful!")
        result.show()
    except Exception as e:
        print(f"Expected error (no real REST service): {e}")

    # Example 4: Demonstrate URI parsing
    print("\n4. URI Parsing Examples:")
    from daft.io.lance.rest_config import parse_lance_uri

    examples = [
        "/local/path/to/lance/data",
        "s3://bucket/lance/data",
        "rest://namespace/table",
        "rest:///root_table",  # Root namespace
        "rest://namespace/schema/table",  # Nested table name
    ]

    for uri in examples:
        uri_type, uri_info = parse_lance_uri(uri)
        print(f"  {uri} -> {uri_type}: {uri_info}")

    # Example 5: Error handling
    print("\n5. Error Handling:")
    try:
        # This should fail because REST URIs require rest_config
        daft.read_lance("rest://namespace/table")
    except ValueError as e:
        print(f"  Correctly caught error: {e}")

    print("\n" + "=" * 40)
    print("Example completed!")
    print("\nTo use with a real REST service:")
    print("1. Replace the base_url with your Lance REST service endpoint")
    print("2. Provide a valid API key")
    print("3. Ensure the lance-namespace package is installed:")
    print("   pip install lance-namespace lance-namespace-urllib3-client")


if __name__ == "__main__":
    main()
