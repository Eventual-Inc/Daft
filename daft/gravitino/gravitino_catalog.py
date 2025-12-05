from __future__ import annotations

import dataclasses
import re
import warnings
from typing import Any, Literal
from urllib.parse import urlparse

from daft.dependencies import requests
from daft.io import AzureConfig, IOConfig, S3Config


@dataclasses.dataclass(frozen=True)
class GravitinoTableInfo:
    """Information about a Gravitino table."""

    name: str
    catalog: str
    schema: str
    table_type: str
    storage_location: str
    format: str
    properties: dict[str, str]


@dataclasses.dataclass(frozen=True)
class GravitinoFilesetInfo:
    """Information about a Gravitino fileset."""

    name: str
    catalog: str
    schema: str
    fileset_type: str
    storage_location: str
    properties: dict[str, str]


@dataclasses.dataclass(frozen=True)
class GravitinoCatalog:
    """Represents a catalog in Gravitino."""

    name: str
    type: str
    provider: str
    properties: dict[str, str]


@dataclasses.dataclass(frozen=True)
class GravitinoTable:
    """Represents a table in Gravitino catalog."""

    table_info: GravitinoTableInfo
    table_uri: str
    io_config: IOConfig | None


@dataclasses.dataclass(frozen=True)
class GravitinoFileset:
    """Represents a fileset in Gravitino catalog."""

    fileset_info: GravitinoFilesetInfo
    io_config: IOConfig | None


class GravitinoClient:
    """Client to access Apache Gravitino catalog.

    Apache Gravitino is an open-source data catalog that provides unified metadata management
    for various data sources and storage systems.

    Example of reading a dataframe from a table in Gravitino:

    >>> client = GravitinoClient("http://localhost:8090", "my_metalake", auth_type="simple", username="admin")
    >>> table = client.load_table("my_catalog.my_schema.my_table")
    >>> df = daft.read_iceberg(table)
    """

    def __init__(
        self,
        endpoint: str,
        metalake_name: str,
        auth_type: Literal["simple", "oauth2"] = "simple",
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
    ):
        """Initialize Gravitino client.

        Args:
            endpoint: Gravitino server endpoint URL
            metalake_name: Name of the metalake to connect to
            auth_type: Authentication type ("simple" or "oauth2")
            username: Username for simple auth
            password: Password for simple auth
            token: OAuth2 token for oauth2 auth
        """
        self._endpoint = endpoint.rstrip("/")
        self._metalake_name = metalake_name
        self._auth_type = auth_type
        self._username = username
        self._password = password
        self._token = token

        # Setup session with authentication
        self._session = requests.Session()
        if auth_type == "simple" and username:
            if password:
                self._session.auth = (username, password)
            else:
                self._session.headers.update({"X-Gravitino-User": username})
        elif auth_type == "oauth2" and token:
            self._session.headers.update({"Authorization": f"Bearer {token}"})

    def _make_request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make HTTP request to Gravitino API."""
        url = f"{self._endpoint}/api{path}"
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def list_catalogs(self) -> list[str]:
        """List all available catalogs in the metalake."""
        try:
            response = self._make_request("GET", f"/metalakes/{self._metalake_name}/catalogs")
            identifiers = response.get("identifiers", [])
            return [identifier.get("name", "") for identifier in identifiers if identifier.get("name")]
        except Exception as e:
            warnings.warn(f"Failed to list catalogs: {e}")
            return []

    def load_catalog(self, catalog_name: str) -> GravitinoCatalog:
        """Load a Gravitino catalog.

        Args:
            catalog_name: Name of the catalog to load

        Returns:
            GravitinoCatalog object

        Raises:
            Exception: If catalog is not found or cannot be loaded
        """
        try:
            response = self._make_request("GET", f"/metalakes/{self._metalake_name}/catalogs/{catalog_name}")
            catalog_data = response.get("catalog", {})

            return GravitinoCatalog(
                name=catalog_data.get("name", catalog_name),
                type=catalog_data.get("type", ""),
                provider=catalog_data.get("provider", ""),
                properties=catalog_data.get("properties", {}),
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise Exception(f"Catalog {catalog_name} not found")
            else:
                raise Exception(f"Failed to load catalog {catalog_name}: {e}")
        except Exception as e:
            raise Exception(f"Failed to load catalog {catalog_name}: {e}")

    def list_namespaces(self, catalog_name: str) -> list[str]:
        """List namespaces in a catalog.

        Note: In Gravitino, a namespace corresponds to a schema. This method lists
        schemas from the Gravitino API and returns them as namespaces in the format
        'catalog_name.schema_name'.
        """
        try:
            response = self._make_request("GET", f"/metalakes/{self._metalake_name}/catalogs/{catalog_name}/schemas")

            # Try new format with identifiers first
            if "identifiers" in response:
                identifiers = response.get("identifiers", [])
                return [
                    f"{catalog_name}.{identifier.get('name', '')}"
                    for identifier in identifiers
                    if identifier.get("name")
                ]

            # Fall back to old format for backward compatibility
            schemas = response.get("schemas", [])
            return [f"{catalog_name}.{schema.get('name', '')}" for schema in schemas if schema.get("name")]
        except Exception as e:
            warnings.warn(f"Failed to list namespaces for catalog {catalog_name}: {e}")
            return []

    def list_tables(self, namespace_name: str) -> list[str]:
        """List tables in a schema."""
        if namespace_name.count(".") != 1:
            raise ValueError(
                f"Expected fully-qualified namespace name with format `catalog_name`.`schema_name`, but received: {namespace_name}"
            )

        catalog_name, schema_name_only = namespace_name.split(".")

        try:
            response = self._make_request(
                "GET", f"/metalakes/{self._metalake_name}/catalogs/{catalog_name}/schemas/{schema_name_only}/tables"
            )

            # Try new format with identifiers first
            if "identifiers" in response:
                identifiers = response.get("identifiers", [])
                return [
                    f"{catalog_name}.{schema_name_only}.{identifier.get('name', '')}"
                    for identifier in identifiers
                    if identifier.get("name")
                ]

            # Fall back to old format for backward compatibility
            tables = response.get("tables", [])
            return [
                f"{catalog_name}.{schema_name_only}.{table.get('name', '')}" for table in tables if table.get("name")
            ]
        except Exception as e:
            warnings.warn(f"Failed to list tables for namespace {namespace_name}: {e}")
            return []

    def load_table(self, table_name: str) -> GravitinoTable:
        """Load an existing Gravitino table.

        Args:
            table_name: Name of the table in the form catalog.schema.table

        Returns:
            GravitinoTable object

        Raises:
            ValueError: If table name format is invalid
            Exception: If table is not found or cannot be loaded
        """
        parts = table_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected table name format 'catalog.schema.table', got: {table_name}")

        catalog_name, schema_name, table_name_only = parts

        try:
            response = self._make_request(
                "GET",
                f"/metalakes/{self._metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name_only}",
            )
            table_data = response.get("table", {})

            # Handle Gravitino 1.0+ API format with storageLocations for tables
            storage_locations = table_data.get("storageLocations", {})
            properties = table_data.get("properties", {})

            # Determine the storage location to use
            storage_location = ""
            if storage_locations:
                # Use the default location specified in properties, or fall back to "default" key
                default_location_name = properties.get("default-location-name", "default")
                storage_location = storage_locations.get(default_location_name, "")

                # If default location not found, use the first available location
                if not storage_location and storage_locations:
                    storage_location = next(iter(storage_locations.values()))
            else:
                # Fallback to old API format for backward compatibility
                storage_location = properties.get("location", "")

            # Convert Gravitino file URL format to Daft-compatible format
            # Gravitino returns "file:/path" but Daft expects "file:///path"
            if storage_location.startswith("file:/") and not storage_location.startswith("file:///"):
                storage_location = storage_location.replace("file:/", "file:///", 1)

            table_info = GravitinoTableInfo(
                name=table_data.get("name", table_name_only),
                catalog=catalog_name,
                schema=schema_name,
                table_type=table_data.get("provider", ""),
                storage_location=storage_location,
                format=properties.get("format", "ICEBERG"),
                properties=properties,
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise Exception(f"Table {table_name} not found")
            else:
                raise Exception(f"Failed to load table {table_name}: {e}")
        except Exception as e:
            raise Exception(f"Failed to load table {table_name}: {e}")

        # Create IO config from table properties
        io_config = _io_config_from_storage_location(table_info.storage_location, table_info.properties)

        return GravitinoTable(
            table_info=table_info,
            table_uri=table_info.storage_location,
            io_config=io_config,
        )

    def load_fileset(self, fileset_name: str) -> GravitinoFileset:
        """Load a Gravitino fileset.

        Args:
            fileset_name: Name of the fileset in the form catalog.schema.fileset

        Returns:
            GravitinoFileset object
        """
        parts = fileset_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected fileset name format 'catalog.schema.fileset', got: {fileset_name}")

        catalog_name, schema_name, fileset_name_only = parts

        try:
            response = self._make_request(
                "GET",
                f"/metalakes/{self._metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name_only}",
            )
            fileset_data = response.get("fileset", {})

            # Handle Gravitino 1.0+ API format with storageLocations
            storage_locations = fileset_data.get("storageLocations", {})
            properties = fileset_data.get("properties", {})

            # Determine the storage location to use
            storage_location = ""
            if storage_locations:
                # Use the default location specified in properties, or fall back to "default" key
                default_location_name = properties.get("default-location-name", "default")
                storage_location = storage_locations.get(default_location_name, "")

                # If default location not found, use the first available location
                if not storage_location and storage_locations:
                    storage_location = next(iter(storage_locations.values()))
            else:
                # Fallback to old API format for backward compatibility
                storage_location = properties.get("location", "")

            # Convert Gravitino URL formats to Daft-compatible formats
            # Gravitino returns "file:/path" but Daft expects "file:///path"
            if storage_location.startswith("file:/") and not storage_location.startswith("file:///"):
                storage_location = storage_location.replace("file:/", "file:///", 1)

            fileset_catalog = self.load_catalog(catalog_name)
            catalog_properties = fileset_catalog.properties

            # Merge catalog properties into fileset properties
            # Fileset properties take precedence over catalog properties
            merged_properties = catalog_properties.copy()
            merged_properties.update(properties)

            fileset_info = GravitinoFilesetInfo(
                name=fileset_data.get("name", fileset_name_only),
                catalog=catalog_name,
                schema=schema_name,
                fileset_type=fileset_data.get("type", "EXTERNAL"),
                storage_location=storage_location,
                properties=merged_properties,
            )

            # Create IO config from fileset properties
            io_config = _io_config_from_storage_location(fileset_info.storage_location, fileset_info.properties)

            return GravitinoFileset(fileset_info=fileset_info, io_config=io_config)

        except Exception as e:
            raise Exception(f"Failed to load fileset {fileset_name}: {e}")


def _io_config_from_storage_location(storage_location: str, properties: dict[str, str]) -> IOConfig | None:
    """Create IOConfig from storage location and properties."""
    scheme = urlparse(storage_location).scheme

    if scheme == "s3" or scheme == "s3a":
        # Extract S3 credentials from properties if available
        access_key = properties.get("s3-access-key-id")
        secret_key = properties.get("s3-secret-access-key")
        endpoint_url = properties.get("s3-endpoint")
        session_token = properties.get("s3-session-token")
        region_name = properties.get("s3-region")

        # Try to extract region from endpoint URL if not explicitly provided
        if not region_name and endpoint_url:
            # Match patterns like "s3.ap-northeast-1.amazonaws.com" or "s3-ap-northeast-1.amazonaws.com"
            region_match = re.search(r"s3[.-]([a-z0-9-]+)\.amazonaws\.com", endpoint_url)
            if region_match:
                region_name = region_match.group(1)

        if access_key and secret_key:
            s3_config = S3Config(
                region_name=region_name,
                key_id=access_key,
                access_key=secret_key,
                endpoint_url=endpoint_url,
                session_token=session_token,
            )

            return IOConfig(s3=s3_config)
        return None
    elif scheme == "gcs" or scheme == "gs":
        # GCS configuration would go here
        warnings.warn("GCS credential configuration from Gravitino is not yet fully supported.")
        return None
    elif scheme == "az" or scheme == "abfs" or scheme == "abfss":
        # Extract Azure credentials from properties if available
        sas_token = properties.get("azure.sas-token")
        if sas_token:
            return IOConfig(azure=AzureConfig(sas_token=sas_token))
        return None
    else:
        warnings.warn(f"Credentials for scheme {scheme} are not yet supported.")
        return None
