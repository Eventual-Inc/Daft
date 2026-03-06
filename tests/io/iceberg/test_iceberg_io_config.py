from __future__ import annotations

from daft.io.iceberg._iceberg import _convert_iceberg_file_io_properties_to_io_config


def test_exact_adls_properties():
    props = {
        "adls.account-name": "myaccount",
        "adls.sas-token": "mytoken",
        "adls.account-key": "mykey",
        "adls.tenant-id": "mytenant",
        "adls.client-id": "myclient",
        "adls.client-secret": "mysecret",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    azure = io_config.azure
    assert azure.storage_account == "myaccount"
    assert azure.sas_token == "mytoken"
    assert azure.access_key == "mykey"
    assert azure.tenant_id == "mytenant"
    assert azure.client_id == "myclient"
    assert azure.client_secret == "mysecret"


def test_scoped_sas_token_extracts_account():
    props = {
        "adls.sas-token.myaccount.dfs.core.windows.net": "scoped-token",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    azure = io_config.azure
    assert azure.sas_token == "scoped-token"
    assert azure.storage_account == "myaccount"


def test_scoped_account_key_extracts_account():
    props = {
        "adls.account-key.myaccount.dfs.core.windows.net": "scoped-key",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    azure = io_config.azure
    assert azure.access_key == "scoped-key"
    assert azure.storage_account == "myaccount"


def test_exact_key_takes_precedence_over_scoped():
    props = {
        "adls.sas-token": "exact-token",
        "adls.sas-token.myaccount.dfs.core.windows.net": "scoped-token",
        "adls.account-name": "exactaccount",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    azure = io_config.azure
    assert azure.sas_token == "exact-token"
    assert azure.storage_account == "exactaccount"


def test_explicit_account_name_not_overridden_by_scoped():
    props = {
        "adls.account-name": "explicit-account",
        "adls.sas-token.scopedaccount.dfs.core.windows.net": "scoped-token",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    azure = io_config.azure
    assert azure.storage_account == "explicit-account"
    assert azure.sas_token == "scoped-token"


def test_empty_props_returns_none():
    io_config = _convert_iceberg_file_io_properties_to_io_config({})
    assert io_config is None


def test_s3_properties_still_work():
    props = {
        "s3.endpoint": "https://s3.example.com",
        "s3.region": "us-east-1",
        "s3.access-key-id": "mykey",
        "s3.secret-access-key": "mysecret",
        "s3.session-token": "mytoken",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    s3 = io_config.s3
    assert s3.endpoint_url == "https://s3.example.com"
    assert s3.region_name == "us-east-1"
    assert s3.key_id == "mykey"
    assert s3.access_key == "mysecret"
    assert s3.session_token == "mytoken"
