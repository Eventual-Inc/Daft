from __future__ import annotations

from daft.daft import IOConfig, S3Config, StorageConfig


def test_storage_config_multiline_display_default():
    """multiline_display() should return only multithreading line when no IO config is set."""
    config = StorageConfig(multithreaded_io=True)

    result = config.multiline_display()
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == "Use multithreading = true"


def test_storage_config_multiline_display_with_io_config():
    """multiline_display() should include IO config line when io_config is provided."""
    s3_config = S3Config(region_name="us-east-1")
    io_config = IOConfig(s3=s3_config)
    config = StorageConfig(multithreaded_io=True, io_config=io_config)

    result = config.multiline_display()
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0].startswith("IO config = ")
    assert "us-east-1" in result[0]
    assert result[1] == "Use multithreading = true"


def test_storage_config_multiline_display_multithreading_false():
    """multiline_display() should reflect multithreaded_io=False."""
    config = StorageConfig(multithreaded_io=False)

    result = config.multiline_display()
    assert len(result) == 1
    assert result[0] == "Use multithreading = false"


def test_storage_config_multiline_display_in_scan_operator_context():
    """Simulate how scan operators use multiline_display() in their list literals."""
    s3_config = S3Config(region_name="us-west-2")
    io_config = IOConfig(s3=s3_config)
    config = StorageConfig(multithreaded_io=True, io_config=io_config)

    # This is how scan operators use it: [..., *config.multiline_display()]
    display_lines = [
        "SomeScanOperator(table_name)",
        "Schema = ...",
        "Partitioning keys = [...]",
        *config.multiline_display(),
    ]

    assert len(display_lines) == 5
    assert display_lines[3].startswith("IO config = ")
    assert display_lines[4] == "Use multithreading = true"
