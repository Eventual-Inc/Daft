# Python IO Extensions

Daft can access custom filesystems implemented in Python through the async [`IOExtension`][daft.io.IOExtension] interface. Register an extension for a URI scheme on [`IOConfig`][daft.io.IOConfig]:

```python
import daft

from daft.io import (
    IOConfig,
    IOExtension,
    IOFileInfo,
    IOFileType,
    IOListing,
    IOReadRange,
)


class MemoryIO(IOExtension):
    def __init__(self, files: dict[str, bytes]):
        self.files = files

    async def supports_range(self, path: str) -> bool:
        return True

    async def get(self, path: str, byte_range: IOReadRange | None = None) -> bytes:
        data = self.files[path]
        if byte_range is None:
            return data
        if byte_range.suffix is not None:
            return data[-byte_range.suffix :]
        return data[byte_range.start : byte_range.end]

    async def put(self, path: str, data: bytes) -> None:
        self.files[path] = data

    async def get_size(self, path: str) -> int:
        return len(self.files[path])

    async def ls(
        self,
        path: str,
        *,
        posix: bool,
        continuation_token: str | None = None,
        page_size: int | None = None,
    ) -> IOListing:
        prefix = path.rstrip("/") + "/"
        return IOListing(
            [
                IOFileInfo(candidate, IOFileType.FILE, len(data))
                for candidate, data in self.files.items()
                if candidate.startswith(prefix)
            ]
        )


path = "memory-demo://bucket/data.csv"
io_config = IOConfig(
    io_extensions={
        "memory-demo": MemoryIO({path: b"id,name\n1,Ada\n"}),
    }
)

df = daft.read_csv(path, io_config=io_config)
```

All required methods raise `NotImplementedError` by default. `get` must return `bytes`, and `ls` must return an `IOListing` containing full URI paths. Raise standard Python exceptions such as `FileNotFoundError`, `PermissionError`, and `IsADirectoryError`; Daft translates them into its native IO error types.

Extension objects must be serializable because Daft stores them in `IOConfig` and may send them to distributed workers. Mutable in-memory state is local to one worker process, so distributed extensions should keep data and shared state in their backing storage rather than on the extension instance. Daft implements globbing and pagination on top of `get_size` and `ls`. Writes through Daft's native writers are buffered before `put` and are limited to 1 GiB per file.
