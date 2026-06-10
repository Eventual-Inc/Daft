"""Python-boundary tests for `daft.CheckpointConfig` and `daft.KeyFilteringSettings`.

Pins the bridge from Python kwargs into the Rust config types: construction
round-trips values, validation rejects zero/negative inputs, and `settings`
is optional with a sensible default.
"""

from __future__ import annotations

import pytest

import daft


def test_key_filtering_settings_round_trip():
    s = daft.KeyFilteringSettings(
        num_workers=7,
        cpus_per_worker=2.5,
        keys_load_batch_size=1024,
        max_concurrency_per_worker=3,
        filter_batch_size=512,
    )
    assert s.num_workers == 7
    assert s.cpus_per_worker == 2.5
    assert s.keys_load_batch_size == 1024
    assert s.max_concurrency_per_worker == 3
    assert s.filter_batch_size == 512


def test_key_filtering_settings_defaults_are_all_none():
    s = daft.KeyFilteringSettings()
    assert s.num_workers is None
    assert s.cpus_per_worker is None
    assert s.keys_load_batch_size is None
    assert s.max_concurrency_per_worker is None
    assert s.filter_batch_size is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"num_workers": 0},
        {"cpus_per_worker": 0.0},
        {"cpus_per_worker": -1.0},
        {"keys_load_batch_size": 0},
        {"max_concurrency_per_worker": 0},
        {"filter_batch_size": 0},
    ],
)
def test_key_filtering_settings_rejects_non_positive(kwargs):
    with pytest.raises(ValueError, match="must be > 0"):
        daft.KeyFilteringSettings(**kwargs)


def test_checkpoint_config_round_trips_settings():
    """User-supplied `settings` must survive the Python → Rust bridge intact.

    Without this, a regression dropping `settings` in `PyCheckpointConfig::new`
    would only surface in production — the optimizer-rule Rust test constructs
    `CheckpointConfig` directly and bypasses the Python entry point.
    """
    store = daft.CheckpointStore("s3://dummy/ckpt")
    config = daft.CheckpointConfig(
        store=store,
        on="file_id",
        settings=daft.KeyFilteringSettings(num_workers=4, cpus_per_worker=2.5),
    )
    assert config._inner.key_column == "file_id"
    s = config._inner.settings
    assert s.num_workers == 4
    assert s.cpus_per_worker == 2.5
    assert s.keys_load_batch_size is None
    assert s.max_concurrency_per_worker is None
    assert s.filter_batch_size is None


def test_checkpoint_config_file_path_mode_when_on_omitted():
    """Omitting `on=` activates file-path mode."""
    store = daft.CheckpointStore("s3://dummy/ckpt")
    config = daft.CheckpointConfig(store=store)
    assert config._inner.is_file_path_mode is True
    assert config._inner.key_column is None


def test_checkpoint_config_row_level_mode_when_on_specified():
    """Specifying `on=` activates row-level mode."""
    store = daft.CheckpointStore("s3://dummy/ckpt")
    config = daft.CheckpointConfig(store=store, on="file_id")
    assert config._inner.is_file_path_mode is False
    assert config._inner.key_column == "file_id"


def test_checkpoint_config_settings_optional_defaults_to_all_none():
    store = daft.CheckpointStore("s3://dummy/ckpt")
    config = daft.CheckpointConfig(store=store, on="file_id")
    s = config._inner.settings
    # Omitting `settings` falls back to a default `KeyFilteringSettings` —
    # all fields `None`, meaning the optimizer rule applies its hardcoded
    # fallback values.
    assert s.num_workers is None
    assert s.cpus_per_worker is None
    assert s.keys_load_batch_size is None
    assert s.max_concurrency_per_worker is None
    assert s.filter_batch_size is None
