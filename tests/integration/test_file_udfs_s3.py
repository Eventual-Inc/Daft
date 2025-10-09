from __future__ import annotations

import pytest

import daft
from daft.functions import file


@pytest.mark.integration()
def test_file_read_from_s3():
    file = daft.File("s3://daft-public-data/tutorials/pdfs/0000000.pdf")
    data = file.read(1)
    assert data == b"%"
    pdf_magic = file.read(3)
    assert pdf_magic == b"PDF"
    file.read(1)  # -
    version = file.read(3)
    assert version == b"1.7"
    file.seek(0)
    whole_thing = file.read(8)
    assert whole_thing == b"%PDF-1.7"
    position = file.tell()
    assert position == 8
    file.seek(0)
    position = file.tell()
    assert position == 0
    file.close()
    assert file.closed()
    try:
        file.read(1)
    except OSError as e:
        assert str(e) == "File not open"


@pytest.mark.integration()
def test_file_read_from_s3_udf():
    df = daft.from_glob_path("s3://daft-public-data/tutorials/pdfs/**/*.pdf").limit(5)

    @daft.func
    def n_bytes(file: daft.File) -> int:
        # This is the most inefficient way to do this. but its just for testing so :shrug:
        return len(file.read())

    actual = df.select(file(df["path"])).select(n_bytes(df["path"])).to_pydict()
    expected = df.select(df["path"].url.download().binary.length()).to_pydict()
    assert actual == expected
