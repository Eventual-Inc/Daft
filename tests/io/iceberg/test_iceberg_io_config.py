from __future__ import annotations

import pytest

from daft import context
from daft.io import AzureConfig, IOConfig, S3Config
from daft.io.iceberg._iceberg import (
    _convert_iceberg_file_io_properties_to_io_config,
    resolve_iceberg_io_config,
)


class _FakeFileIO:
    def __init__(self, properties: dict[str, str]):
        self.properties = properties


class _FakeTable:
    """Minimal stand-in for a PyIceberg Table exposing only what the resolver reads."""

    def __init__(self, properties: dict[str, str], location: str):
        self.io = _FakeFileIO(properties)
        self._location = location

    def location(self) -> str:
        return self._location


def test_oss_location_applies_settings():
    """An oss:// table location enables virtual-hosted addressing and the oss->s3 alias."""
    props = {"s3.endpoint": "http://oss-cn-hangzhou.aliyuncs.com", "s3.access-key-id": "ak"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "oss://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is True
    assert result.protocol_aliases == {"oss": "s3"}
    # Table properties are still applied.
    assert result.s3.endpoint_url == "http://oss-cn-hangzhou.aliyuncs.com"
    assert result.s3.key_id == "ak"


def test_oss_location_no_props():
    """An oss:// table with no IO properties still gets the OSS settings.

    Covers credentials supplied via the environment rather than table properties.
    """
    result = _convert_iceberg_file_io_properties_to_io_config({}, "oss://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is True
    assert result.protocol_aliases == {"oss": "s3"}


def test_non_oss_location_with_props():
    """A non-oss:// location with properties leaves the OSS settings off."""
    props = {"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is False
    assert result.protocol_aliases == {}
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_non_oss_location_no_props_returns_none():
    """A non-oss:// location with no properties yields no IOConfig."""
    assert _convert_iceberg_file_io_properties_to_io_config({}, "s3://my-bucket/warehouse/db/table") is None


def test_no_location_with_props():
    """With no location (default), properties still convert and OSS settings stay off."""
    props = {"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.s3.force_virtual_addressing is False
    assert result.protocol_aliases == {}
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_no_location_no_props_returns_none():
    """With no location (default) and no properties, no IOConfig is produced."""
    assert _convert_iceberg_file_io_properties_to_io_config({}) is None


@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
def test_azure_scoped_sas_token(prefix):
    props = {f"{prefix}.sas-token.testaccount.dfs.core.windows.net": "vended-sas"}
    original = props.copy()
    table = _FakeTable(props, "abfss://container@testaccount.dfs.core.windows.net/table")

    result = resolve_iceberg_io_config(table)

    assert result is not None
    assert result.azure.storage_account == "testaccount"
    assert result.azure.sas_token == "vended-sas"
    assert props == original


@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
@pytest.mark.parametrize("scoped_prefix", ["adls", "adlfs"])
def test_azure_unscoped_sas_token_takes_precedence(prefix, scoped_prefix):
    props = {
        f"{prefix}.account-name": "explicitaccount",
        f"{prefix}.sas-token": "explicit-sas",
        f"{scoped_prefix}.sas-token.explicitaccount.dfs.core.windows.net": "vended-sas",
    }
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.azure.storage_account == "explicitaccount"
    assert result.azure.sas_token == "explicit-sas"


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("plain_sas", [False, True])
@pytest.mark.parametrize("second_host", ["second.dfs.core.windows.net", "first.blob.core.windows.net"])
@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
def test_azure_multiple_scoped_sas_tokens(explicit, plain_sas, second_host, prefix):
    props = {
        "adls.sas-token.first.dfs.core.windows.net": "first-sas",
        f"{prefix}.sas-token.{second_host}": "second-sas",
    }
    if plain_sas:
        props.update({"adls.account-name": "first", "adls.sas-token": "plain-sas"})
    io_config = IOConfig(azure=AzureConfig(storage_account="first", sas_token="explicit-sas")) if explicit else None
    table = _FakeTable(props, "az://container/table")
    if explicit or plain_sas:
        result = resolve_iceberg_io_config(table, io_config)
        assert result is not None
        assert result.azure.storage_account == "first"
        assert result.azure.sas_token == ("explicit-sas" if explicit else "plain-sas")
    else:
        assert _convert_iceberg_file_io_properties_to_io_config(props) is None


def test_azure_ambiguous_scoped_sas_preserves_other_properties():
    props = {
        "adls.sas-token.first.dfs.core.windows.net": "first-sas",
        "adlfs.sas-token.second.dfs.core.windows.net": "second-sas",
        "s3.region": "eu-west-1",
    }
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.azure.storage_account is None
    assert result.azure.sas_token is None
    assert result.s3.region_name == "eu-west-1"


@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
@pytest.mark.parametrize("account", ["first", "second"])
def test_azure_explicit_account_selects_scoped_sas(prefix, account):
    props = {
        f"{prefix}.account-name": account,
        "adls.sas-token.first.dfs.core.windows.net": "first-sas",
        "adlfs.sas-token.second.dfs.core.windows.net": "second-sas",
    }
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.azure.storage_account == account
    assert result.azure.sas_token == f"{account}-sas"


@pytest.mark.parametrize("plain_sas", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
def test_azure_unmatched_scoped_sas_is_ignored(plain_sas, explicit, prefix):
    props = {
        f"{prefix}.account-name": "differentaccount",
        f"{prefix}.sas-token.testaccount.dfs.core.windows.net": "vended-sas",
    }
    if plain_sas:
        props[f"{prefix}.sas-token"] = "plain-sas"
    io_config = IOConfig() if explicit else None
    table = _FakeTable(props, "az://container/table")
    if explicit:
        assert resolve_iceberg_io_config(table, io_config) is io_config
    else:
        result = resolve_iceberg_io_config(table)
        assert result is not None
        assert result.azure.storage_account == "differentaccount"
        assert result.azure.sas_token == ("plain-sas" if plain_sas else None)


@pytest.mark.parametrize("account", [None, "testaccount", "differentaccount"])
@pytest.mark.parametrize("prefix", ["adls", "adlfs"])
def test_azure_explicit_io_config_with_scoped_sas(account, prefix):
    props = {f"{prefix}.sas-token.testaccount.dfs.core.windows.net": "vended-sas"}
    table = _FakeTable(props, "az://container/table")
    explicit = IOConfig(azure=AzureConfig(storage_account=account, sas_token="explicit-sas"))
    assert resolve_iceberg_io_config(table, explicit) is explicit


def test_explicit_io_config_does_not_access_table_properties():
    explicit = IOConfig(s3=S3Config(region_name="us-east-1"))
    # Any access to table.io or table.location would fail on this stand-in.
    assert resolve_iceberg_io_config(object(), explicit) is explicit


def test_azure_scoped_sas_aliases_for_same_host():
    props = {
        "adlfs.sas-token.testaccount.dfs.core.windows.net": "legacy-sas",
        "adls.sas-token.testaccount.dfs.core.windows.net": "vended-sas",
    }
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.azure.storage_account == "testaccount"
    assert result.azure.sas_token == "vended-sas"


def test_resolve_explicit_io_config_wins():
    """An explicitly-provided IOConfig takes precedence over table props and the default."""
    explicit = IOConfig(s3=S3Config(region_name="us-east-1"))
    table = _FakeTable({"s3.region": "eu-west-1"}, "s3://bucket/warehouse/db/table")
    result = resolve_iceberg_io_config(table, explicit)
    assert result is explicit


def test_resolve_falls_back_to_table_properties():
    """With no explicit IOConfig, the table's FileIO properties are translated."""
    table = _FakeTable({"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}, "s3://bucket/warehouse/db/table")
    result = resolve_iceberg_io_config(table, None)
    assert result is not None
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_resolve_falls_back_to_default_io_config():
    """With no explicit IOConfig and no table props, the context default_io_config is used."""
    default = IOConfig(s3=S3Config(region_name="ap-south-1"))
    table = _FakeTable({}, "s3://bucket/warehouse/db/table")
    old = context.get_context().daft_planning_config.default_io_config
    context.set_planning_config(default_io_config=default)
    try:
        result = resolve_iceberg_io_config(table, None)
        assert result is not None
        assert result.s3.region_name == "ap-south-1"
    finally:
        context.set_planning_config(default_io_config=old)
