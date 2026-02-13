from __future__ import annotations

import os

import pytest

from daft.catalog import Catalog
from daft.unity_catalog import OAuth2Credentials, UnityCatalog


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Unity Catalog OAuth tests require the `--credentials` flag")
    if not os.getenv("DATABRICKS_ENDPOINT"):
        pytest.skip(reason="Unity Catalog OAuth tests require the DATABRICKS_ENDPOINT environment variable")
    if not os.getenv("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID"):
        pytest.skip(reason="Unity Catalog OAuth tests require the DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID environment variable")
    if not os.getenv("DATABRICKS_SERVICE_PRINCIPAL_SECRET"):
        pytest.skip(reason="Unity Catalog OAuth tests require the DATABRICKS_SERVICE_PRINCIPAL_SECRET environment variable")


@pytest.mark.integration()
def test_unity_catalog_oauth_credentials_connection() -> None:
    endpoint = os.getenv("DATABRICKS_ENDPOINT", "")

    client_id = os.getenv("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_SERVICE_PRINCIPAL_SECRET", "")

    unity_catalog = UnityCatalog(
        endpoint=endpoint,
        oauth=OAuth2Credentials(client_id=client_id, client_secret=client_secret),
    )
    catalog = Catalog.from_unity(unity_catalog)

    # Smoke test for OAuth-backed Unity Catalog connection setup.
    assert isinstance(catalog.list_tables(), list)
