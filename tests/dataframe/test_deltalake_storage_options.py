from __future__ import annotations

import pytest

from daft.dataframe.dataframe import _configure_deltalake_storage_options


def test_deltalake_s3_uses_default_conditional_put_for_modern_versions():
    storage_options = {"region": "us-west-2"}

    changed = _configure_deltalake_storage_options(
        "s3://bucket/table",
        storage_options,
        dynamo_table_name=None,
        allow_unsafe_rename=False,
        deltalake_version="1.6.0",
    )

    assert not changed
    assert storage_options == {"region": "us-west-2"}


def test_deltalake_s3_dynamodb_locking_is_opt_in():
    storage_options = {"region": "us-west-2"}

    changed = _configure_deltalake_storage_options(
        "s3://bucket/table",
        storage_options,
        dynamo_table_name="delta-locks",
        allow_unsafe_rename=False,
        deltalake_version="1.6.0",
    )

    assert changed
    assert storage_options == {
        "region": "us-west-2",
        "AWS_S3_LOCKING_PROVIDER": "dynamodb",
        "DELTA_DYNAMO_TABLE_NAME": "delta-locks",
    }


def test_deltalake_s3_unsafe_rename_is_explicit():
    storage_options = {"region": "us-west-2"}

    changed = _configure_deltalake_storage_options(
        "s3://bucket/table",
        storage_options,
        dynamo_table_name=None,
        allow_unsafe_rename=True,
        deltalake_version="1.6.0",
    )

    assert changed
    assert storage_options == {
        "region": "us-west-2",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


@pytest.mark.skip(reason="This test will be enabled for daft versions >= 0.8.0")
def test_deltalake_s3_old_versions_require_explicit_locking_choice():
    with pytest.raises(ValueError, match="deltalake>=0.23.0"):
        _configure_deltalake_storage_options(
            "s3://bucket/table",
            {},
            dynamo_table_name=None,
            allow_unsafe_rename=False,
            deltalake_version="0.22.3",
        )


def test_deltalake_file_unsafe_rename_is_preserved():
    storage_options: dict[str, str] = {}

    changed = _configure_deltalake_storage_options(
        "file:///tmp/table",
        storage_options,
        dynamo_table_name=None,
        allow_unsafe_rename=True,
        deltalake_version="1.6.0",
    )

    assert changed
    assert storage_options == {"MOUNT_ALLOW_UNSAFE_RENAME": "true"}
