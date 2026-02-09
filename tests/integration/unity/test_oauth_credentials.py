from __future__ import annotations

import os

import pytest

from daft.catalog import Catalog
from daft.unity_catalog import OAuth2Credentials, UnityCatalog


@pytest.mark.integration()
@pytest.mark.skipif(not os.getenv("DATABRICKS_ENDPOINT"), reason="requires DATABRICKS_ENDPOINT")
@pytest.mark.skipif(
    not os.getenv("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID"), reason="requires DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID"
)
@pytest.mark.skipif(
    not os.getenv("DATABRICKS_SERVICE_PRINCIPAL_SECRET"), reason="requires DATABRICKS_SERVICE_PRINCIPAL_SECRET"
)
def test_unity_catalog_oauth_credentials_connection() -> None:
    endpoint = os.getenv("DATABRICKS_ENDPOINT", "")
    # if not endpoint:
    #    pytest.skip("requires DATABRICKS_ENDPOINT")

    client_id = os.getenv("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_SERVICE_PRINCIPAL_SECRET", "")
    # if not client_id or not client_secret:
    #    pytest.skip("requires DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID and DATABRICKS_SERVICE_PRINCIPAL_SECRET")

    unity_catalog = UnityCatalog(
        endpoint=endpoint,
        oauth=OAuth2Credentials(client_id=client_id, client_secret=client_secret),
    )
    catalog = Catalog.from_unity(unity_catalog)

    # Smoke test for OAuth-backed Unity Catalog connection setup.
    assert isinstance(catalog.list_tables(), list)
