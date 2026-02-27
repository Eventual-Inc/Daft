# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


@dataclass
class LanceRestConfig:
    """Configuration for Lance REST Namespace connections.

    Args:
        base_url: Base URL of the Lance REST service (e.g., "https://api.lancedb.com")
        api_key: Optional API key for authentication
        timeout: Request timeout in seconds (default: 30)
        headers: Additional HTTP headers to include in requests
    """

    base_url: str
    api_key: str | None = None
    timeout: int = 30
    headers: dict[str, str] | None = None


def parse_lance_uri(uri: str) -> tuple[str, dict[str, Any]]:
    """Parse Lance URI to determine if it's REST-based or file-based.

    Args:
        uri: URI to parse. Can be:
            - File path: "/path/to/lance/data/" or "s3://bucket/path"
            - REST URI: "rest://namespace/table_name"

    Returns:
        Tuple of (uri_type, uri_info) where:
        - uri_type is "rest" or "file"
        - uri_info contains parsed components
    """
    if uri.startswith("rest://"):
        # Parse rest://namespace/table_name format
        parsed = urlparse(uri)
        path_parts = parsed.path.lstrip("/").split("/")

        if len(path_parts) < 1 or not path_parts[0]:
            raise ValueError(f"Invalid REST URI format: {uri}. Expected rest://namespace/table_name")

        table_name = path_parts[0] if len(path_parts) == 1 else "/".join(path_parts)

        return "rest", {
            "namespace": parsed.netloc or "$",  # Default to root namespace
            "table_name": table_name,
        }
    else:
        # Existing file/object store path
        return "file", {"path": uri}
