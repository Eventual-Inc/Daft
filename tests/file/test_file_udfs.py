from __future__ import annotations

import io
from pathlib import Path

import daft
from daft import DataType as dt
from daft.functions import file


def test_can_convert_string_to_file_type():
    df = daft.from_pydict({"paths": ["./some_file.txt"]})
    assert df.schema() == daft.Schema.from_pydict({"paths": dt.string()})

    df = df.select(file(df["paths"]))

    assert df.schema() == daft.Schema.from_pydict({"paths": dt.file()})


def test_can_convert_bytes_to_file_type():
    df = daft.from_pydict({"data": [b"hello world"]})
    assert df.schema() == daft.Schema.from_pydict({"data": dt.binary()})

    df = df.select(file(df["data"]))
    assert df.schema() == daft.Schema.from_pydict({"data": dt.file()})


def test_can_open_local_file(tmp_path: Path):
    # Create a file in the temporary directory
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("test content")

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})

    df = df.select(file(df["path"]))

    @daft.func
    def read_text(file: daft.File) -> str:
        with open(file) as f:
            return f.read()

    df = df.select(read_text(df["path"]).alias("text"))
    assert df.to_pydict()["text"] == ["test content"]


def test_can_open_local_image_with_pil(tmp_path: Path):
    import numpy as np
    from PIL import Image

    # Create a simple red image
    img_array = np.zeros((100, 100, 3), dtype=np.uint8)
    img_array[:, :, 0] = 255  # Red channel
    img = Image.fromarray(img_array)

    # Save to temp file
    temp_file = tmp_path / "test_image.png"
    img.save(temp_file)

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})

    df = df.select(file(df["path"]))

    @daft.func(return_dtype=dt.bool())
    def open_with_pil(file: daft.File):
        from PIL import Image

        img = Image.open(file)
        return img.getpixel((0, 0))[0] == 255

    df = df.select(open_with_pil(df["path"]).alias("is_red_image"))
    assert df.to_pydict()["is_red_image"] == [True]


def test_can_open_in_memory_image_with_pil():
    import numpy as np
    from PIL import Image

    img_array = np.zeros((100, 100, 3), dtype=np.uint8)
    img_array[:, :, 2] = 255
    img = Image.fromarray(img_array)

    # Save to bytes buffer
    img_buffer = io.BytesIO()
    img.save(img_buffer, format="PNG")
    img_bytes = img_buffer.getvalue()

    df = daft.from_pydict({"data": [img_bytes]})

    df = df.select(file(df["data"]))
    df.show()

    @daft.func(return_dtype=dt.bool())
    def open_with_pil(file: daft.File):
        from PIL import Image

        img = Image.open(file)
        return img.getpixel((0, 0))[2] == 255

    df = df.select(open_with_pil(df["data"]).alias("is_blue_image"))
    assert df.to_pydict()["is_blue_image"] == [True]


def test_large_binary_file_handling():
    large_data = b"\x00" * 1024 * 1024
    df = daft.from_pydict({"data": [large_data]})
    df = df.select(file(df["data"]))

    @daft.func
    def get_size(file: daft.File) -> int:
        return len(file.read())

    @daft.func
    def sample(file: daft.File) -> bytes:
        file.seek(1024 * 512)  # Middle
        return file.read(10)

    df = df.select(get_size(df["data"]).alias("size"), sample(df["data"]).alias("sample"))

    results = df.to_pydict()
    assert results["size"][0] == 1024 * 1024
    assert results["sample"][0] == b"\x00" * 10


def test_large_binary_file_handling_with_file(tmp_path: Path):
    large_data = b"\x00" * 1024 * 1024  # 1MB of zeros
    binary_file = tmp_path / "large_binary.dat"
    binary_file.write_bytes(large_data)

    df = daft.from_pydict({"path": [str(binary_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func
    def get_size(file: daft.File) -> int:
        return len(file.read())

    @daft.func
    def sample(file: daft.File) -> bytes:
        file.seek(1024 * 512)  # Middle
        return file.read(10)

    df = df.select(get_size(df["path"]).alias("size"), sample(df["path"]).alias("sample"))

    results = df.to_pydict()
    assert results["size"][0] == 1024 * 1024
    assert results["sample"][0] == b"\x00" * 10


def test_compatibility_with_json():
    import json

    json_data = '{"name": "Alice", "skills": ["Python", "Rust"]}'

    df = daft.from_pydict({"data": [json_data.encode()]})
    df = df.select(file(df["data"]))

    @daft.func()
    def read_with_json(file: daft.File) -> str:
        data = json.load(file)
        return data["skills"][0]

    df = df.select(read_with_json(df["data"]).alias("skill"))
    assert df.to_pydict()["skill"] == ["Python"]


def test_compatibility_with_json_file(tmp_path: Path):
    import json

    json_data = '{"name": "Alice", "skills": ["Python", "Rust"]}'
    json_file = tmp_path / "data.json"
    json_file.write_text(json_data)

    df = daft.from_pydict({"path": [str(json_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func()
    def read_with_json(file: daft.File) -> str:
        data = json.load(file)
        return data["skills"][0]

    df = df.select(read_with_json(df["path"]).alias("skill"))
    assert df.to_pydict()["skill"] == ["Python"]


def test_with_open_syntax_for_path_file(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello from file")

    df = daft.from_pydict({"path": [str(test_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func
    def read(file: daft.File) -> str:
        with file as f:
            return f.read()

    df = df.select(read(df["path"]).alias("content"))
    assert df.to_pydict()["content"] == ["Hello from file"]


def test_with_open_syntax_for_memory_file():
    data = "Hello from memory"

    df = daft.from_pydict({"data": [data.encode()]})
    df = df.select(file(df["data"]))

    @daft.func
    def read(file: daft.File) -> str:
        with file as f:
            return f.read().decode("utf-8")

    df = df.select(read(df["data"]).alias("content"))
    assert df.to_pydict()["content"] == ["Hello from memory"]
