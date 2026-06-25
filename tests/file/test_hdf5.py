from __future__ import annotations

import pytest

pytest.importorskip("h5py")

import numpy as np

import daft
from daft.file.hdf5 import HDF5_DEFAULT_BUFFER_SIZE
from daft.functions.hdf5 import (
    _field_name_from_dataset,
    _normalize_datasets,
    hdf5_keys_impl,
    hdf5_read_impl,
    hdf5_read_many_impl,
    hdf5_visit_impl,
)


@pytest.fixture
def sample_hdf5_path(tmp_path):
    import h5py

    path = tmp_path / "sample.h5"
    with h5py.File(path, "w") as f:
        # Root and dataset attrs cover the lightweight metadata/attrs accessors.
        f.attrs["source"] = "unit-test"
        f.create_dataset("values", data=np.array([1.0, 2.0, 3.0]))
        f["values"].attrs["unit"] = "meters"

        # A nested group+dataset covers keys(), visit(), metadata(), and
        # read/read_many for hierarchical dataset paths.
        action = f.create_group("action")
        action.create_dataset("proprio", data=np.zeros((4, 7)))

        # An empty group keeps group traversal behavior distinct from datasets.
        f.create_group("observation")
    return str(path)


def test_hdf5_file_standalone(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    assert file.keys() == ["action", "observation", "values"]
    assert file.keys(group="action") == ["proprio"]


def test_hdf5_file_open_uses_hdf5_buffer_default():
    assert daft.Hdf5File.open.__defaults__ == (HDF5_DEFAULT_BUFFER_SIZE,)


def test_hdf5_file_read_many_and_attrs(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)
    data = file.read({"values": "values", "proprio": "action/proprio"})

    assert np.allclose(data["values"], [1.0, 2.0, 3.0])
    assert data["proprio"].shape == (4, 7)
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


def test_hdf5_file_post_init_validates_mimetype(sample_hdf5_path, monkeypatch):
    file = daft.Hdf5File(sample_hdf5_path)
    monkeypatch.setattr(file, "is_hdf5", lambda: False)

    with pytest.raises(ValueError, match="not an HDF5 file"):
        file.__post_init__()


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
    assert result["keys"][0] == ["action", "observation", "values"]


def test_hdf5_read_expression(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(daft.functions.hdf5_file(daft.col("path")).alias("trajectory")).with_column(
        "values",
        daft.functions.hdf5_read(daft.col("trajectory"), dataset="values"),
    )

    result = df.collect().to_pydict()
    assert np.allclose(result["values"][0], [1.0, 2.0, 3.0])


def test_hdf5_read_many_expression(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(
        daft.functions.hdf5_read_many(
            daft.functions.hdf5_file(daft.col("path")),
            {"values": "values", "proprio": "action/proprio"},
        ).alias("datasets")
    )

    result = df.collect().to_pydict()
    assert np.allclose(result["datasets"][0]["values"], [1.0, 2.0, 3.0])
    assert result["datasets"][0]["proprio"].shape == (4, 7)


def test_hdf5_visit_expression(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    df = df.select(
        daft.functions.hdf5_visit(
            daft.functions.hdf5_file(daft.col("path")),
            group="/",
        ).alias("objects")
    )

    objects = df.collect().to_pydict()["objects"][0]
    assert "action/proprio" in objects


def test_hdf5_function_impls(sample_hdf5_path):
    file = daft.Hdf5File(sample_hdf5_path)

    assert hdf5_keys_impl(file, group="/") == ["action", "observation", "values"]
    assert "action/proprio" in hdf5_visit_impl(file, group="/")
    assert np.allclose(hdf5_read_impl(file, dataset="values"), [1.0, 2.0, 3.0])
    data = hdf5_read_many_impl(file, datasets={"values": "values"})
    assert np.allclose(data["values"], [1.0, 2.0, 3.0])


def test_hdf5_read_many_normalizes_dataset_inputs(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})

    single = df.select(
        daft.functions.hdf5_read_many(
            daft.functions.hdf5_file(daft.col("path")),
            "values",
        ).alias("datasets")
    )
    assert np.allclose(single.collect().to_pydict()["datasets"][0]["values"], [1.0, 2.0, 3.0])

    many = df.select(
        daft.functions.hdf5_read_many(
            daft.functions.hdf5_file(daft.col("path")),
            ["values", "action/proprio"],
        ).alias("datasets")
    )
    result = many.collect().to_pydict()["datasets"][0]
    assert np.allclose(result["values"], [1.0, 2.0, 3.0])
    assert result["action_proprio"].shape == (4, 7)


def test_hdf5_read_many_rejects_invalid_dataset_inputs():
    assert _field_name_from_dataset("/") == "dataset"
    assert _normalize_datasets({"values": "values"}) == {"values": "values"}

    with pytest.raises(ValueError, match="duplicate output field names"):
        _normalize_datasets(["action/proprio", "action-proprio"])
    with pytest.raises(ValueError, match="at least one"):
        _normalize_datasets([])
    with pytest.raises(TypeError, match="string aliases"):
        _normalize_datasets({1: "values"})
    with pytest.raises(TypeError, match="string aliases"):
        _normalize_datasets({"values": 1})


def test_as_hdf5_from_generic_file(sample_hdf5_path):
    df = daft.from_pydict({"path": [sample_hdf5_path]})
    file_col = daft.functions.file(df["path"])
    assert daft.File(sample_hdf5_path).as_hdf5().keys() == ["action", "observation", "values"]
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
