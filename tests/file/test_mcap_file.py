from __future__ import annotations

import pytest

pytest.importorskip("mcap")

from mcap.writer import Writer

import daft
from daft.exceptions import DaftCoreException


@pytest.fixture
def sample_mcap_path(tmp_path):
    path = tmp_path / "sample.mcap"
    with path.open("wb") as output:
        writer = Writer(output)
        writer.start()
        writer.finish()
    return str(path)


def test_mcap_file_type(sample_mcap_path):
    file = daft.McapFile(sample_mcap_path)
    assert file.is_mcap()

    df = daft.from_pydict({"path": [sample_mcap_path]}).select(
        daft.functions.mcap_file(daft.col("path"), verify=True).alias("recording")
    )
    assert df.schema()["recording"].dtype == daft.DataType.file(daft.MediaType.mcap())
    assert isinstance(df.collect().to_pydict()["recording"][0], daft.McapFile)


def test_as_mcap_from_generic_file(sample_mcap_path):
    assert isinstance(daft.File(sample_mcap_path).as_mcap(), daft.McapFile)

    df = daft.from_pydict({"path": [sample_mcap_path]})
    df = df.select(daft.functions.mcap_file(daft.functions.file(df["path"]), verify=True))
    assert isinstance(df.collect().to_pydict()["path"][0], daft.McapFile)


def test_mcap_file_rejects_invalid_magic(tmp_path):
    path = tmp_path / "not-mcap.mcap"
    path.write_bytes(b"not an mcap")

    with pytest.raises(ValueError, match="not an MCAP file"):
        daft.McapFile(str(path))

    df = daft.from_pydict({"path": [str(path)]})
    with pytest.raises(DaftCoreException, match="Invalid MCAP file"):
        df.select(daft.functions.mcap_file(daft.col("path"), verify=True)).collect()
