from __future__ import annotations

import os

import pytest

import daft


def test_upload_local(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].url.upload(str(tmpdir + "/nested")))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_single_file_url(tmpdir):
    bytes_data = [b"a"]
    paths = [f"{tmpdir}/0"]
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    # Even though there is only one row, since we pass in the upload URL via an expression, we
    # should treat the given path as a per-row path and write directly to that path, instead of
    # treating the path as a directory and writing to `{path}/uuid`.
    df = df.with_column("files", df["data"].url.upload(df["paths"]))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected
    # Check that data was uploaded to the correct paths.
    for path, expected in zip(results["files"], paths):
        assert path == "file://" + expected


def test_upload_local_row_specifc_urls(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    paths = [f"{tmpdir}/0", f"{tmpdir}/1", f"{tmpdir}/2"]
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].url.upload(df["paths"]))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected
    # Check that data was uploaded to the correct paths.
    for path, expected in zip(results["files"], paths):
        assert path == "file://" + expected


@pytest.mark.skipif(os.geteuid() == 0, reason="Skipping test when run as root user")
def test_upload_local_no_write_permissions(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    # We have no write permissions to the first and third paths.
    paths = ["/some-root-path", f"{tmpdir}/normal_path", "/another-bad-path"]
    expected_paths = [None, f"file://{tmpdir}/normal_path", None]
    expected_data = b"b"
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    df_raise_error = df.with_column("files", df["data"].url.upload(df["paths"]))
    with pytest.raises(daft.exceptions.DaftCoreException):
        df_raise_error.collect()
    # Retry with `on_error` set to `null`.
    df_null = df.with_column("files", df["data"].url.upload(df["paths"], on_error="null"))
    df_null.collect()
    results = df_null.to_pydict()
    for path, expected_path in zip(results["files"], expected_paths):
        assert (path is None and expected_path is None) or path == expected_path
        if path is not None:
            assert path.startswith("file://")
            path = path[len("file://") :]
            with open(path, "rb") as f:
                assert f.read() == expected_data


def test_upload_local_with_png_template(tmpdir):
    """Test upload with .png filename template in location."""
    bytes_data = [b"image1", b"image2", b"image3"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    
    # Upload with .png template
    df = df.with_column("files", df["data"].url.upload(f"{tmpdir}/{{}}.png"))
    df.collect()
    
    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        
        # Check that filename ends with .png
        filename = os.path.basename(path)
        assert filename.endswith(".png"), f"Expected .png extension, got {filename}"
        
        # Verify file content
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_with_custom_prefix_template(tmpdir):
    """Test upload with custom prefix and extension template."""
    bytes_data = [b"content1", b"content2", b"content3"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    
    # Upload with custom prefix template
    df = df.with_column("files", df["data"].url.upload(f"{tmpdir}/image_{{}}.jpg"))
    df.collect()
    
    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        
        # Check that filename has correct prefix and extension
        filename = os.path.basename(path)
        assert filename.startswith("image_"), f"Expected 'image_' prefix, got {filename}"
        assert filename.endswith(".jpg"), f"Expected .jpg extension, got {filename}"
        
        # Verify file content
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_with_doc_template(tmpdir):
    """Test upload with document template using daft.functions.upload."""
    bytes_data = [b"doc1", b"doc2", b"doc3"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    
    # Upload with document template using daft.functions.upload
    from daft.functions import upload
    df = df.with_column("files", upload(df["data"], f"{tmpdir}/doc_{{}}.txt"))
    df.collect()
    
    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        
        # Check that filename has correct prefix and extension
        filename = os.path.basename(path)
        assert filename.startswith("doc_"), f"Expected 'doc_' prefix, got {filename}"
        assert filename.endswith(".txt"), f"Expected .txt extension, got {filename}"
        
        # Verify file content
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_with_nested_template(tmpdir):
    """Test upload with template in nested directory structure."""
    bytes_data = [b"nested1", b"nested2"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    
    # Create nested directory structure
    nested_dir = f"{tmpdir}/nested/subdir"
    os.makedirs(nested_dir, exist_ok=True)
    
    # Upload with template in nested path
    df = df.with_column("files", df["data"].url.upload(f"{nested_dir}/file_{{}}.json"))
    df.collect()
    
    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        
        # Check that filename has correct prefix and extension
        filename = os.path.basename(path)
        assert filename.startswith("file_"), f"Expected 'file_' prefix, got {filename}"
        assert filename.endswith(".json"), f"Expected .json extension, got {filename}"
        
        # Check that file is in the correct nested directory
        assert "nested/subdir" in path, f"Expected nested path, got {path}"
        
        # Verify file content
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_template_vs_no_template(tmpdir):
    """Test that template and non-template uploads work correctly."""
    bytes_data = [b"test1", b"test2"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    
    # Upload without template (default behavior)
    df_no_template = df.with_column("files_no_template", df["data"].url.upload(f"{tmpdir}/no_template"))
    df_no_template.collect()
    
    # Upload with template
    df_with_template = df.with_column("files_with_template", df["data"].url.upload(f"{tmpdir}/with_template/{{}}.png"))
    df_with_template.collect()
    
    results_no_template = df_no_template.to_pydict()
    results_with_template = df_with_template.to_pydict()
    
    # Check no-template uploads
    for path in results_no_template["files_no_template"]:
        assert path.startswith("file://")
        path = path.removeprefix("file://")
        filename = os.path.basename(path)
        # Should not have .png extension (default UUID filename)
        assert not filename.endswith(".png"), f"Unexpected .png extension in no-template upload: {filename}"
        assert "no_template" in path, f"Expected 'no_template' in path: {path}"
    
    # Check template uploads
    for path in results_with_template["files_with_template"]:
        assert path.startswith("file://")
        path = path.removeprefix("file://")
        filename = os.path.basename(path)
        # Should have .png extension
        assert filename.endswith(".png"), f"Expected .png extension in template upload: {filename}"
        assert "with_template" in path, f"Expected 'with_template' in path: {path}"
