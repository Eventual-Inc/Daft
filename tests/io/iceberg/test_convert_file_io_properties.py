"""Tests for `_convert_iceberg_file_io_properties_to_io_config`.

Focused on the ADLS account-scoped property key handling. PyIceberg (when used
with Polaris and other catalogs that vend per-account credentials) returns
scoped keys like `adls.sas-token.<account>.dfs.core.windows.net` instead of
the plain `adls.sas-token` key. See issue #6357.
"""

from __future__ import annotations

from daft.io.iceberg._iceberg import _convert_iceberg_file_io_properties_to_io_config


def test_unscoped_sas_token_matches():
    """Backward compat: plain unscoped keys still work."""
    props = {"adls.sas-token": "plain-token"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.sas_token == "plain-token"


def test_unscoped_account_key_matches():
    props = {"adls.account-key": "plain-key"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.access_key == "plain-key"


def test_scoped_sas_token_extracts_value_and_account():
    """Polaris-vended credentials: scoped key yields both value and account name."""
    props = {"adls.sas-token.myaccount.dfs.core.windows.net": "scoped-token"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.sas_token == "scoped-token"
    assert io_config.azure.storage_account == "myaccount"


def test_scoped_account_key_extracts_value_and_account():
    props = {"adls.account-key.myaccount.dfs.core.windows.net": "scoped-key"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.access_key == "scoped-key"
    assert io_config.azure.storage_account == "myaccount"


def test_adlfs_prefix_scoped_also_works():
    """Both `adls.*` and `adlfs.*` prefixes should accept scoped keys."""
    props = {"adlfs.sas-token.myaccount.dfs.core.windows.net": "scoped-token"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.sas_token == "scoped-token"
    assert io_config.azure.storage_account == "myaccount"


def test_explicit_account_name_overrides_scoped_key_suffix():
    """Explicit `adls.account-name` beats a scoped-key suffix.

    If both are present and disagree, the explicit value wins.
    """
    props = {
        "adls.account-name": "explicit-account",
        "adls.sas-token.other-account.dfs.core.windows.net": "scoped-token",
    }
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.storage_account == "explicit-account"
    assert io_config.azure.sas_token == "scoped-token"


def test_unscoped_account_name_with_unscoped_sas_token():
    """Fully unscoped: account-name and sas-token both explicit."""
    props = {"adls.account-name": "myacct", "adls.sas-token": "plain-token"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.azure.storage_account == "myacct"
    assert io_config.azure.sas_token == "plain-token"


def test_no_azure_props_returns_none_if_no_other_props():
    """Function returns None only when NO relevant props of any kind are set."""
    io_config = _convert_iceberg_file_io_properties_to_io_config({"unrelated.key": "x"})
    assert io_config is None


def test_s3_props_still_work():
    """Existing non-Azure handling is untouched."""
    props = {"s3.access-key-id": "AKIA", "s3.secret-access-key": "secret"}
    io_config = _convert_iceberg_file_io_properties_to_io_config(props)
    assert io_config is not None
    assert io_config.s3.key_id == "AKIA"
    assert io_config.s3.access_key == "secret"
