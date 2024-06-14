from __future__ import annotations

import dataclasses

import requests

from daft.io import IOConfig, S3Config


@dataclasses.dataclass(frozen=True)
class UnityCatalogTable:
    table_uri: str
    io_config: IOConfig | None


class UnityCatalog:
    def __init__(self, endpoint: str, token: str | None = None):
        self._endpoint = endpoint
        self._token_header = {"Authorization": f"Bearer {token}"} if token else {}

    def list_schemas(self):
        raise NotImplementedError("Listing schemas not yet implemented.")

    def list_tables(self, schema: str):
        raise NotImplementedError("Listing tables not yet implemented.")

    def load_table(self, name: str) -> UnityCatalogTable:
        # Load the table ID
        table_info = requests.get(
            self._endpoint + f"/api/2.1/unity-catalog/tables/{name}", headers=self._token_header
        ).json()
        table_id = table_info["table_id"]
        table_uri = table_info["storage_location"]

        # Grab credentials from Unity catalog and place it into the Table
        temp_table_cred_endpoint = self._endpoint + "/api/2.1/unity-catalog/temporary-table-credentials"
        response = requests.post(
            temp_table_cred_endpoint, json={"table_id": table_id, "operation": "READ"}, headers=self._token_header
        )

        aws_temp_credentials = response.json()["aws_temp_credentials"]
        io_config = (
            IOConfig(
                s3=S3Config(
                    key_id=aws_temp_credentials.get("access_key_id"),
                    access_key=aws_temp_credentials.get("secret_access_key"),
                    session_token=aws_temp_credentials.get("session_token"),
                )
            )
            if aws_temp_credentials is not None
            else None
        )

        return UnityCatalogTable(
            table_uri=table_uri,
            io_config=io_config,
        )
