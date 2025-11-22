"""Shared utilities for Gravitino integration tests."""

from __future__ import annotations

_API_HEADERS = {
    "Accept": "application/vnd.gravitino.v1+json",
    "Content-Type": "application/json",
}


def api_request(client, method: str, path: str, **kwargs):
    """Helper to make Gravitino REST API requests."""
    url = f"{client._endpoint.rstrip('/')}/api{path}"
    response = client._session.request(method, url, headers=_API_HEADERS, timeout=30, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


def ensure_metalake(client, metalake: str):
    """Ensure metalake exists, create if it doesn't."""
    url = f"{client._endpoint.rstrip('/')}/api/metalakes/{metalake}"
    response = client._session.get(url, headers=_API_HEADERS, timeout=30)
    if response.status_code == 404:
        payload = {
            "name": metalake,
            "comment": "Daft integration metalake",
            "properties": {},
        }
        api_request(client, "POST", "/metalakes", json=payload)
    else:
        response.raise_for_status()


def set_catalog_in_use(client, metalake: str, catalog_name: str, in_use: bool):
    """Set the catalog's in-use property."""
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}"
    try:
        payload = {"inUse": in_use}
        api_request(client, "PATCH", path, json=payload)
    except Exception:
        pass


def delete_catalog(client, metalake: str, catalog_name: str):
    """Delete a catalog, setting in-use to false first."""
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}"
    try:
        # Set in-use to false before deleting
        set_catalog_in_use(client, metalake, catalog_name, False)
        api_request(client, "DELETE", path)
    except Exception:
        pass


def delete_schema(client, metalake: str, catalog_name: str, schema_name: str, cascade: bool = False):
    """Delete a schema."""
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}"
    if cascade:
        path += "?cascade=true"
    try:
        api_request(client, "DELETE", path)
    except Exception:
        pass
