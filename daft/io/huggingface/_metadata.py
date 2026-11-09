from __future__ import annotations

import fnmatch
import json
import re
import warnings
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlencode

from daft.daft import HTTPConfig
from daft.file import open_file

if TYPE_CHECKING:
    from daft.daft import IOConfig


def read_json(url: str, io_config: IOConfig, *, viewer: bool = False) -> dict[str, Any]:
    """Read HF metadata using the same native IO backend as the data readers."""
    if viewer:
        # datasets-server is a different host, so it uses HTTPConfig, not HuggingFaceConfig.
        http = io_config.http
        io_config = io_config.replace(
            http=HTTPConfig(
                bearer_token=None if io_config.hf.anonymous else io_config.hf.token,
                retry_initial_backoff_ms=http.retry_initial_backoff_ms,
                connect_timeout_ms=http.connect_timeout_ms,
                read_timeout_ms=http.read_timeout_ms,
                num_tries=http.num_tries,
            )
        )
    with open_file(url, io_config=io_config) as handle:
        result = json.load(handle)
    if not isinstance(result, dict):
        raise TypeError(f"Expected a metadata object from {url}")
    return result


def repo_info(repo: str, revision: str | None, io_config: IOConfig) -> dict[str, Any]:
    path = f"https://huggingface.co/api/datasets/{repo}"
    if revision is not None:
        path += f"/revision/{quote(revision, safe='')}"
    return read_json(path, io_config)


def repo_root(repo: str, revision: str | None) -> str:
    root = f"hf://datasets/{repo}"
    return root if revision is None else f"{root}@{quote(revision, safe='')}"


def parquet_files(
    repo: str, config_name: str | None, split: str | None, allow_partial: bool, io_config: IOConfig
) -> list[str]:
    metadata = read_json(
        f"https://datasets-server.huggingface.co/parquet?{urlencode({'dataset': repo})}", io_config, viewer=True
    )
    files = metadata.get("parquet_files")
    if not isinstance(files, list):
        raise TypeError(f"Invalid Parquet metadata for {repo!r}: missing parquet_files")

    def matches(entry: dict[str, Any]) -> bool:
        return (config_name is None or entry.get("config") == config_name) and (
            split is None or entry.get("split") == split
        )

    # Never read a completed subset of a requested conversion as if it were complete.
    if any(matches(entry) for key in ("pending", "failed") for entry in metadata.get(key, [])):
        raise ValueError(f"Parquet conversion for the requested selection in {repo!r} is pending or failed")

    if not files:
        raise FileNotFoundError(f"No converted Parquet files are available for {repo!r}")

    configs = {entry["config"] for entry in files}
    if config_name is not None and config_name not in configs:
        raise ValueError(f"Unknown config_name {config_name!r} for {repo!r}. Available: {sorted(configs)}")
    selected = [entry for entry in files if matches(entry)]
    if not selected:
        if split is not None and files:
            splits = {entry["split"] for entry in files if config_name is None or entry["config"] == config_name}
            raise ValueError(f"Unknown split {split!r} for {repo!r}. Available: {sorted(splits)}")
        raise FileNotFoundError(f"No converted Parquet files are available for {repo!r}")

    def is_partial(entry: dict[str, Any]) -> bool:
        return any(part.startswith("partial-") for part in unquote(entry["url"]).split("/"))

    # If the API reports partial data without identifying its files, fail conservatively.
    partial = any(is_partial(entry) for entry in selected) or (
        metadata.get("partial", False) and not any(is_partial(entry) for entry in files)
    )
    if partial:
        message = (
            f"The Parquet conversion for {repo!r} is partial and does not contain the complete requested dataset. "
            "Use format='datasets' to load the original source, or allow_partial=True to explicitly read partial data."
        )
        if not allow_partial:
            raise ValueError(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    return list(dict.fromkeys(entry["url"] for entry in selected))


def source_files(
    repo: str,
    extension: str,
    config_name: str | None,
    split: str | None,
    data_dir: str | None,
    revision: str | None,
    io_config: IOConfig,
) -> list[str]:
    """Select original files from dataset-card declarations or standard split filenames."""
    info = repo_info(repo, revision, io_config)
    files = [entry["rfilename"] for entry in info.get("siblings", []) if entry["rfilename"].endswith(extension)]
    if not files:
        raise FileNotFoundError(f"No original {extension} files are available for {repo!r}")
    configs = (info.get("cardData") or {}).get("configs") or []
    patterns: list[str] | None = None
    if configs and (data_dir is None or config_name is not None or split is not None):
        candidates = [entry for entry in configs if config_name is None or entry["config_name"] == config_name]
        if not candidates:
            raise ValueError(f"Unknown config_name {config_name!r}. Available: {[c['config_name'] for c in configs]}")
        patterns = []
        for config in candidates:
            declarations = config.get("data_files", [])
            if isinstance(declarations, str):
                declarations = [{"split": "train", "path": declarations}]
            elif isinstance(declarations, dict):
                declarations = [{"split": key, "path": value} for key, value in declarations.items()]
            for declaration in declarations:
                if isinstance(declaration, str):
                    declaration = {"split": "train", "path": declaration}
                if split is None or declaration["split"] == split:
                    path = declaration["path"]
                    patterns.extend([path] if isinstance(path, str) else path)
        files = [path for path in files if any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)]
    elif data_dir is None or config_name is not None or split is not None:
        if config_name not in (None, "default"):
            raise ValueError("This repository does not declare configs; use data_dir to select original files")
        if split is not None:
            # Recognize HF's standard split directories and split-prefixed filenames, not substring matches.
            boundary = re.compile(rf"(^|/){re.escape(split)}(?:/|[-_.])")
            files = [path for path in files if boundary.search(path)]
    if data_dir is not None:
        prefix = data_dir.strip("/") + "/"
        files = [path for path in files if path.startswith(prefix)]
    if not files:
        raise FileNotFoundError(f"No {extension} files match the requested config/split/data_dir in {repo!r}")
    root = repo_root(repo, revision)
    return [f"{root}/{path}" for path in sorted(set(files))]
