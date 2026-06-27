from __future__ import annotations

import pytest

pytest.importorskip("h5py")

import numpy as np

import daft
from daft.file.hdf5 import HDF5_DEFAULT_BUFFER_SIZE, HDF5_SCAN_BUFFER_SIZE
from daft.functions.hdf5 import hdf5_keys_impl

SAMPLE_HDF5_ROOT_KEYS = ["action", "float32_values", "int_values", "observation", "values"]


@pytest.fixture
def sample_hdf5_path(tmp_path):
    import h5py

    path = tmp_path / "sample.h5"
    with h5py.File(path, "w") as f:
        # Root and dataset attrs cover the lightweight metadata/attrs accessors.
        f.attrs["source"] = "unit-test"
        f.create_dataset("values", data=np.array([1.0, 2.0, 3.0]))
        f["values"].attrs["unit"] = "meters"
        f.create_dataset("int_values", data=np.array([1, 2, 3], dtype=np.int32))
        f.create_dataset("float32_values", data=np.array([1.0, 2.0, 3.0], dtype=np.float32))

        # A nested group+dataset covers keys(), visit(), metadata(), and
        # direct reads for hierarchical dataset paths.
        action = f.create_group("action")
        action.create_dataset("proprio", data=np.zeros((4, 7)))

        # An empty group keeps group traversal behavior distinct from datasets.
        f.create_group("observation")
    return str(path)


def test_hdf5_file_standalone(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    assert file.keys() == SAMPLE_HDF5_ROOT_KEYS
    assert file.keys(group="action") == ["proprio"]


def test_hdf5_file_open_uses_hdf5_buffer_default():
    assert daft.Hdf5File.open.__defaults__ == (HDF5_DEFAULT_BUFFER_SIZE,)


def test_hdf5_file_metadata_methods_use_scan_buffer(sample_hdf5_path, monkeypatch):
    file = daft.Hdf5File(sample_hdf5_path)
    original_open_h5py = file._open_h5py
    seen_buffer_sizes: list[int | None] = []

    def track_open_h5py(buffer_size=HDF5_DEFAULT_BUFFER_SIZE):
        seen_buffer_sizes.append(buffer_size)
        return original_open_h5py(buffer_size)

    monkeypatch.setattr(file, "_open_h5py", track_open_h5py)

    assert file.keys() == SAMPLE_HDF5_ROOT_KEYS
    assert file.attrs()["source"] == "unit-test"
    assert "action/proprio" in file.visit()
    assert any(obj["h5path"] == "action/proprio" for obj in file.metadata())

    assert seen_buffer_sizes == [HDF5_SCAN_BUFFER_SIZE] * 4


def test_hdf5_file_read_sequence_and_attrs(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    data = file.read(["values", "action/proprio"])

    assert np.allclose(data["values"], [1.0, 2.0, 3.0])
    assert data["action/proprio"].shape == (4, 7)
    assert file.attrs()["source"] == "unit-test"
    assert file.attrs(h5path="values")["unit"] == "meters"


def test_hdf5_file_read_single_and_sequence(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)

    assert np.allclose(file.read("values"), [1.0, 2.0, 3.0])

    data = file.read(["values", "action/proprio"])
    assert np.allclose(data["values"], [1.0, 2.0, 3.0])
    assert data["action/proprio"].shape == (4, 7)


def test_hdf5_file_read_group_errors(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)

    with pytest.raises(TypeError, match="not an HDF5 dataset"):
        file.read("action")


def test_hdf5_file_visit(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    names = file.visit()
    seen: list[str] = []
    result = file.visit(seen.append)

    assert "action/proprio" in names
    assert result is None
    assert "action/proprio" in seen
    assert file.visit(lambda name: name if name == "action/proprio" else None) == "action/proprio"


def test_hdf5_file_metadata(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    objects = file.metadata()
    assert {"h5path": "action", "kind": "group", "shape": [], "dtype": "", "chunks": [], "compression": ""} in objects
    assert any(obj["h5path"] == "action/proprio" and obj["shape"] == [4, 7] for obj in objects)


def test_hdf5_file_metadata_group_and_dataset_errors(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)

    assert file.metadata(group="action") == [
        {
            "h5path": "action/proprio",
            "kind": "dataset",
            "shape": [4, 7],
            "dtype": "float64",
            "chunks": [],
            "compression": "",
        }
    ]
    with pytest.raises(TypeError, match="not an HDF5 group"):
        file.metadata(group="values")


def test_hdf5_file_dependency_errors(sample_hdf5_path, monkeypatch):
    import daft.file.hdf5 as hdf5_module

    monkeypatch.setattr(hdf5_module.h5py, "module_available", lambda: False)
    with pytest.raises(ImportError, match="daft\\[hdf5\\]"):
        daft.Hdf5File(sample_hdf5_path)

    monkeypatch.setattr(hdf5_module.h5py, "module_available", lambda: True)
    monkeypatch.setattr(hdf5_module.np, "module_available", lambda: False)
    with pytest.raises(ImportError, match="numpy"):
        daft.Hdf5File(sample_hdf5_path)


def test_hdf5_file_init_validates_mimetype(tmp_path):
    path = tmp_path / "not-hdf5.txt"
    path.write_text("not an hdf5 file")

    with pytest.raises(ValueError, match="not an HDF5 file"):
        daft.Hdf5File(str(path))


def test_hdf5_file_dtype(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(daft.functions.hdf5_file(df["path"]).alias("trajectory"))

    field = next(df.schema().__iter__())
    assert field.dtype == daft.DataType.file(daft.MediaType.hdf5())


def test_hdf5_keys_expression(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(daft.functions.hdf5_file(daft.col("path")).alias("trajectory")).with_column(
        "keys",
        daft.functions.hdf5_keys(daft.col("trajectory"), group="/"),
    )

    result = df.collect().to_pydict()
    assert result["keys"][0] == SAMPLE_HDF5_ROOT_KEYS


def test_hdf5_metadata_expression(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(daft.functions.hdf5_file(daft.col("path")).alias("trajectory")).with_column(
        "metadata",
        daft.functions.hdf5_metadata(daft.col("trajectory"), group="/"),
    )

    objects = df.collect().to_pydict()["metadata"][0]
    assert {"h5path": "action", "kind": "group", "shape": [], "dtype": "", "chunks": [], "compression": ""} in objects
    assert any(obj["h5path"] == "action/proprio" and obj["shape"] == [4, 7] for obj in objects)

    expr_df = daft.from_pydict({"path": [sample_hdf5_path]})
    expr_df = expr_df.select(daft.functions.hdf5_file(daft.col("path")).alias("trajectory")).with_column(
        "metadata", daft.col("trajectory").hdf5_metadata(group="action")
    )
    action_objects = expr_df.collect().to_pydict()["metadata"][0]
    assert action_objects == [
        {
            "h5path": "action/proprio",
            "kind": "dataset",
            "shape": [4, 7],
            "dtype": "float64",
            "chunks": [],
            "compression": "",
        }
    ]


def test_hdf5_function_impls(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)

    assert hdf5_keys_impl(file, group="/") == SAMPLE_HDF5_ROOT_KEYS


def test_as_hdf5_from_generic_file(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    file_col = daft.functions.file(df["path"])
    assert daft.File(sample_hdf5_path).as_hdf5().keys() == SAMPLE_HDF5_ROOT_KEYS
    df = df.select(daft.functions.hdf5_file(file_col, verify=True).alias("trajectory"))
    df.collect()


def test_hdf5_file_verify_user_block_without_extension(tmp_path):
    import h5py

    path = tmp_path / "sample-with-user-block"
    with h5py.File(path, "w", userblock_size=512) as f:
        f.create_dataset("values", data=np.array([1.0, 2.0, 3.0]))

    df = daft.from_pydict({"path": [str(path)]})
    df = df.select(daft.functions.hdf5_file(daft.col("path"), verify=True).alias("trajectory"))
    df.collect()
