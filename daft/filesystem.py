from fsspec import AbstractFileSystem, get_filesystem_class


def get_filesystem(protocol: str, **kwargs) -> AbstractFileSystem:
    klass = get_filesystem_class(protocol)
    fs = klass(**kwargs)
    return fs


def get_filesystem_from_path(path: str, **kwargs) -> AbstractFileSystem:
    split = path.split(":")
    assert len(split) <= 2, f"too many colons found in {path}"
    protocol = split[0] if len(split) == 2 else "file"
    return get_filesystem(protocol, **kwargs)
