"""Shared utilities for Gravitino integration tests."""

from __future__ import annotations

_API_HEADERS = {
    "Accept": "application/vnd.gravitino.v1+json",
    "Content-Type": "application/json",
}


def api_request(client, method: str, path: str, **kwargs):
    """Helper to make Gravitino REST API requests."""
    url = f"{client._endpoint.rstrip('/')}/api{path}"
    print(f"[DEBUG] API Request: {method} {url}")
    response = client._session.request(method, url, headers=_API_HEADERS, timeout=30, **kwargs)
    try:
        response.raise_for_status()
        print(f"[DEBUG] API Response: {response.status_code} - Success")
    except Exception:
        # Log response body for debugging
        try:
            error_detail = response.json()
            print(f"[DEBUG] Gravitino API error: {error_detail}")
        except Exception:
            print(f"[DEBUG] Gravitino API error (raw): {response.text}")
        raise
    return response.json() if response.content else {}


def ensure_metalake(client, metalake: str):
    """Ensure metalake exists, create if it doesn't."""
    print(f"[DEBUG] Ensuring metalake exists: {metalake}")
    url = f"{client._endpoint.rstrip('/')}/api/metalakes/{metalake}"
    response = client._session.get(url, headers=_API_HEADERS, timeout=30)
    if response.status_code == 404:
        print(f"[DEBUG] Metalake not found, creating: {metalake}")
        payload = {
            "name": metalake,
            "comment": "Daft integration metalake",
            "properties": {},
        }
        api_request(client, "POST", "/metalakes", json=payload)
        print(f"[DEBUG] Metalake created: {metalake}")
    else:
        response.raise_for_status()
        print(f"[DEBUG] Metalake already exists: {metalake}")


def set_catalog_in_use(client, metalake: str, catalog_name: str, in_use: bool):
    """Set the catalog's in-use property."""
    print(f"[DEBUG] Setting catalog in-use={in_use}: {catalog_name}")
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}"
    try:
        payload = {"inUse": in_use}
        api_request(client, "PATCH", path, json=payload)
        print(f"[DEBUG] Catalog in-use updated: {catalog_name}")
    except Exception as e:
        print(f"[DEBUG] Failed to set catalog in-use (ignoring): {e}")
        pass


def delete_catalog(client, metalake: str, catalog_name: str):
    """Delete a catalog, setting in-use to false first."""
    print(f"[DEBUG] Deleting catalog: {catalog_name}")
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}"
    try:
        # Set in-use to false before deleting
        set_catalog_in_use(client, metalake, catalog_name, False)
        api_request(client, "DELETE", path)
        print(f"[DEBUG] Catalog deleted: {catalog_name}")
    except Exception as e:
        print(f"[DEBUG] Failed to delete catalog (ignoring): {e}")
        pass


def delete_schema(client, metalake: str, catalog_name: str, schema_name: str, cascade: bool = False):
    """Delete a schema."""
    print(f"[DEBUG] Deleting schema: {schema_name} (cascade={cascade})")
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}"
    if cascade:
        path += "?cascade=true"
    try:
        api_request(client, "DELETE", path)
        print(f"[DEBUG] Schema deleted: {schema_name}")
    except Exception as e:
        print(f"[DEBUG] Failed to delete schema (ignoring): {e}")
        pass


def create_catalog(
    client,
    metalake: str,
    catalog_name: str,
    catalog_type: str = "FILESET",
    comment: str | None = None,
    properties: dict | None = None,
):
    """Create a catalog.

    Args:
        catalog_type: Catalog type (e.g., "FILESET", "RELATIONAL"). Must be uppercase.
    """
    print(f"[DEBUG] Creating catalog: {catalog_name} (type={catalog_type})")
    print(f"[DEBUG] Catalog properties: {properties}")
    payload = {
        "name": catalog_name,
        "type": catalog_type,
        "comment": comment or f"Daft integration {catalog_type.lower()} catalog",
        "properties": properties or {},
    }
    api_request(client, "POST", f"/metalakes/{metalake}/catalogs", json=payload)
    print(f"[DEBUG] Catalog created: {catalog_name}")


def update_catalog(client, metalake: str, catalog_name: str, updates: list[dict]):
    """Update a catalog using the alter-catalog REST API.

    Args:
        updates: List of update operations. Each operation is a dict with "@type" and other fields.
                 Example: [{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9000"}]
    """
    print(f"[DEBUG] Updating catalog: {catalog_name}")
    print(f"[DEBUG] Update operations: {updates}")
    payload = {"updates": updates}
    api_request(client, "PUT", f"/metalakes/{metalake}/catalogs/{catalog_name}", json=payload)
    print(f"[DEBUG] Catalog updated: {catalog_name}")


def create_schema(client, metalake: str, catalog_name: str, schema_name: str, comment: str | None = None):
    """Create a schema."""
    print(f"[DEBUG] Creating schema: {schema_name} in catalog {catalog_name}")
    payload = {
        "name": schema_name,
        "comment": comment or "Daft integration schema",
        "properties": {},
    }
    api_request(client, "POST", f"/metalakes/{metalake}/catalogs/{catalog_name}/schemas", json=payload)
    print(f"[DEBUG] Schema created: {schema_name}")


def create_fileset(
    client,
    metalake: str,
    catalog_name: str,
    schema_name: str,
    fileset_name: str,
    storage_uri: str,
    comment: str | None = None,
):
    """Create a fileset."""
    print(f"[DEBUG] Creating fileset: {fileset_name} with storage URI: {storage_uri}")
    payload = {
        "name": fileset_name,
        "type": "EXTERNAL",
        "comment": comment or "Daft integration fileset",
        "properties": {
            "default-location-name": "default",
            "location": storage_uri,
        },
        "storageLocations": {"default": storage_uri},
    }
    api_request(
        client,
        "POST",
        f"/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}/filesets",
        json=payload,
    )
    print(f"[DEBUG] Fileset created: {fileset_name}")


def delete_fileset(client, metalake: str, catalog_name: str, schema_name: str, fileset_name: str):
    """Delete a fileset."""
    print(f"[DEBUG] Deleting fileset: {fileset_name}")
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}"
    try:
        api_request(client, "DELETE", path)
        print(f"[DEBUG] Fileset deleted: {fileset_name}")
    except Exception as e:
        print(f"[DEBUG] Failed to delete fileset (ignoring): {e}")
        pass
