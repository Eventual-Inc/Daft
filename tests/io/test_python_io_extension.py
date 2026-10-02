from __future__ import annotations

import asyncio
import pickle
from pathlib import Path
from urllib.parse import urlsplit

import pytest

import daft
from daft.daft import io_get, io_glob, io_put
from daft.io import IOConfig, IOExtension, IOFileInfo, IOFileType, IOListing, IOReadRange


class InMemoryIOExtension(IOExtension):
    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = files or {}

    async def supports_range(self, path: str) -> bool:
        return True

    async def get(self, path: str, byte_range: IOReadRange | None = None) -> bytes:
        try:
            data = self.files[path]
        except KeyError as error:
            raise FileNotFoundError(path) from error
        if byte_range is None:
            return data
        if byte_range.suffix is not None:
            return data[-byte_range.suffix :]
        assert byte_range.start is not None
        return data[byte_range.start : byte_range.end]

    async def put(self, path: str, data: bytes) -> None:
        self.files[path] = data

    async def get_size(self, path: str) -> int:
        try:
            return len(self.files[path])
        except KeyError as error:
            prefix = f"{path.rstrip('/')}/"
            if any(candidate.startswith(prefix) for candidate in self.files):
                raise IsADirectoryError(path) from error
            raise FileNotFoundError(path) from error

    async def ls(
        self,
        path: str,
        *,
        posix: bool,
        continuation_token: str | None = None,
        page_size: int | None = None,
    ) -> IOListing:
        if continuation_token is not None:
            return IOListing([])
        if path in self.files:
            return IOListing([IOFileInfo(path, IOFileType.FILE, len(self.files[path]))])

        prefix = f"{path.rstrip('/')}/"
        entries: dict[str, IOFileInfo] = {}
        for candidate, data in self.files.items():
            if not candidate.startswith(prefix):
                continue
            relative = candidate[len(prefix) :]
            if posix and "/" in relative:
                directory = relative.split("/", 1)[0]
                directory_path = f"{prefix}{directory}/"
                entries[directory_path] = IOFileInfo(directory_path, IOFileType.DIRECTORY)
            else:
                entries[candidate] = IOFileInfo(candidate, IOFileType.FILE, len(data))
        return IOListing(list(entries.values()), not_found_if_empty=True)

    async def delete(self, path: str) -> None:
        self.files.pop(path, None)


class DirectoryIOExtension(IOExtension):
    def __init__(self, root: Path) -> None:
        self.root = root

    def _local_path(self, path: str) -> Path:
        parsed = urlsplit(path)
        return self.root / parsed.netloc / parsed.path.lstrip("/")

    def _extension_path(self, path: Path, *, is_dir: bool) -> str:
        relative = path.relative_to(self.root)
        bucket, *parts = relative.parts
        suffix = "/".join(parts)
        result = f"directory-test://{bucket}/{suffix}"
        return f"{result}/" if is_dir else result

    async def supports_range(self, path: str) -> bool:
        return True

    async def get(self, path: str, byte_range: IOReadRange | None = None) -> bytes:
        data = self._local_path(path).read_bytes()
        if byte_range is None:
            return data
        if byte_range.suffix is not None:
            return data[-byte_range.suffix :]
        assert byte_range.start is not None
        return data[byte_range.start : byte_range.end]

    async def put(self, path: str, data: bytes) -> None:
        local_path = self._local_path(path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)

    async def get_size(self, path: str) -> int:
        local_path = self._local_path(path)
        if local_path.is_dir():
            raise IsADirectoryError(path)
        return local_path.stat().st_size

    async def ls(
        self,
        path: str,
        *,
        posix: bool,
        continuation_token: str | None = None,
        page_size: int | None = None,
    ) -> IOListing:
        if continuation_token is not None:
            return IOListing([])
        local_path = self._local_path(path)
        if local_path.is_file():
            return IOListing([IOFileInfo(path, IOFileType.FILE, local_path.stat().st_size)])
        if not local_path.exists():
            return IOListing([], not_found_if_empty=True)

        candidates = local_path.iterdir() if posix else local_path.rglob("*")
        files = [
            IOFileInfo(
                self._extension_path(candidate, is_dir=candidate.is_dir()),
                IOFileType.DIRECTORY if candidate.is_dir() else IOFileType.FILE,
                None if candidate.is_dir() else candidate.stat().st_size,
            )
            for candidate in candidates
        ]
        return IOListing(files)

    async def delete(self, path: str) -> None:
        self._local_path(path).unlink(missing_ok=True)


def test_required_methods_raise_not_implemented() -> None:
    extension = IOExtension()

    async def invoke_methods() -> None:
        with pytest.raises(NotImplementedError):
            await extension.supports_range("test://file")
        with pytest.raises(NotImplementedError):
            await extension.get("test://file")
        with pytest.raises(NotImplementedError):
            await extension.put("test://file", b"")
        with pytest.raises(NotImplementedError):
            await extension.get_size("test://file")
        with pytest.raises(NotImplementedError):
            await extension.ls("test://", posix=True)
        with pytest.raises(NotImplementedError):
            await extension.delete("test://file")

    asyncio.run(invoke_methods())


def test_io_extension_config_is_serializable() -> None:
    config = IOConfig(io_extensions={"memory-test": InMemoryIOExtension({"memory-test://bucket/a": b"a"})})
    restored = pickle.loads(pickle.dumps(config))

    assert isinstance(restored.io_extensions["memory-test"], InMemoryIOExtension)
    assert "memory-test" in repr(restored)
    assert "memory-test" in restored.replace().io_extensions
    assert restored.replace(io_extensions={}).io_extensions == {}


@pytest.mark.parametrize("scheme", ["", "has space", "1starts-with-number", "contains_underscore"])
def test_io_extension_rejects_invalid_scheme(scheme: str) -> None:
    with pytest.raises(ValueError, match="Invalid IO extension URI scheme"):
        IOConfig(io_extensions={scheme: InMemoryIOExtension()})


def test_io_extension_cannot_share_scheme_with_alias() -> None:
    with pytest.raises(ValueError, match="both a protocol alias and an IO extension"):
        IOConfig(
            protocol_aliases={"memory-test": "file"},
            io_extensions={"memory-test": InMemoryIOExtension()},
        )


def test_io_extension_read_write_ranges_and_glob() -> None:
    config = IOConfig(
        io_extensions={
            "memory-test": InMemoryIOExtension(
                {
                    "memory-test://bucket/data/a.txt": b"abcdef",
                    "memory-test://bucket/data/nested/b.txt": b"nested",
                    "memory-test://bucket/data/ignored.bin": b"ignored",
                }
            )
        }
    )

    assert io_get("memory-test://bucket/data/a.txt", io_config=config) == b"abcdef"
    assert (
        io_get(
            "memory-test://bucket/data/a.txt",
            io_config=config,
            range_start=1,
            range_end=4,
        )
        == b"bcd"
    )
    assert io_get("memory-test://bucket/data/a.txt", io_config=config, suffix=2) == b"ef"

    io_put("memory-test://bucket/data/new.txt", b"new", io_config=config)
    assert io_get("memory-test://bucket/data/new.txt", io_config=config) == b"new"
    io_put(
        "memory-test://bucket/data/cross-client.txt",
        b"shared",
        multithreaded_io=True,
        io_config=config,
    )
    assert (
        io_get(
            "memory-test://bucket/data/cross-client.txt",
            multithreaded_io=False,
            io_config=config,
        )
        == b"shared"
    )

    files = io_glob("memory-test://bucket/data/**/*.txt", io_config=config)
    assert {file["path"] for file in files} == {
        "memory-test://bucket/data/a.txt",
        "memory-test://bucket/data/nested/b.txt",
        "memory-test://bucket/data/new.txt",
        "memory-test://bucket/data/cross-client.txt",
    }


def test_io_extension_preserves_not_found_errors() -> None:
    config = IOConfig(io_extensions={"memory-test": InMemoryIOExtension()})

    with pytest.raises(FileNotFoundError, match="missing"):
        io_get("memory-test://bucket/missing", io_config=config)


def test_io_extension_integrates_with_daft_readers() -> None:
    path = "memory-test://bucket/data.csv"
    config = IOConfig(io_extensions={"memory-test": InMemoryIOExtension({path: b"id,name\n1,Ada\n2,Lin\n"})})

    assert daft.read_csv(path, io_config=config).to_pydict() == {
        "id": [1, 2],
        "name": ["Ada", "Lin"],
    }


def test_io_extension_integrates_with_daft_writers(tmp_path: Path) -> None:
    config = IOConfig(io_extensions={"directory-test": DirectoryIOExtension(tmp_path)})
    output = "directory-test://bucket/output"

    daft.from_pydict({"id": [1, 2], "name": ["Ada", "Lin"]}).write_parquet(output, io_config=config)

    assert daft.read_parquet(f"{output}/*.parquet", io_config=config).to_pydict() == {
        "id": [1, 2],
        "name": ["Ada", "Lin"],
    }
