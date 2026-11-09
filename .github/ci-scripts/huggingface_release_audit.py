from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from importlib.metadata import distribution
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "tests/release/huggingface/catalog.json"
FORMATS = {"webdataset", "json", "csv", "parquet", "optimized-parquet", "imagefolder", "audiofolder", "text", "arrow"}
MAX_REPOSITORY_BYTES = 50_000_000


def load_catalog(path: Path = CATALOG) -> list[dict]:
    cases = json.loads(path.read_text())
    if not isinstance(cases, list):
        raise TypeError("Catalog must be a list")
    if Counter(case["category"] for case in cases) != Counter(dict.fromkeys(FORMATS, 5)):
        raise ValueError("Catalog must have five cases for each of the nine formats")
    for category in FORMATS:
        if len({case["repo"] for case in cases if case["category"] == category}) != 5:
            raise ValueError("Each format must cover five different repositories")
    if len({case["id"] for case in cases}) != len(cases):
        raise ValueError("Case IDs must be unique")
    for case in cases:
        if not re.fullmatch(r"[a-z0-9-]+", case["id"]):
            raise ValueError("Case IDs must be filesystem-safe")
        if not re.fullmatch(r"[0-9a-f]{40}", case["kwargs"]["revision"]):
            raise ValueError(f"{case['id']} must pin an immutable repository commit")
        if case["kwargs"].get("allow_partial", False):
            raise ValueError("Partial reads cannot satisfy the release audit")
        if case["kwargs"].get("format") not in ("parquet", "datasets", "webdataset"):
            raise ValueError("Every case must declare its read path")
    return cases


def _retry(fn):
    # Reuse the existing typed Daft/Python network classification, but never skip.
    spec = importlib.util.spec_from_file_location("hf_release_retry", ROOT / "tests/_hf_retry.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.call_with_hf_retry(fn, retries=2, backoff_seconds=3, on_exhausted="raise")


def _media_bytes(value, cache: Path) -> bytes:
    import daft

    if isinstance(value, daft.File):
        with value.open() as handle:
            return handle.read()
    if isinstance(value, dict):
        if value.get("bytes") is not None:
            return value["bytes"]
        location = value.get("path")
        if location and location.startswith(("hf://datasets/", "https://huggingface.co/")):
            with daft.open_file(location, "rb") as handle:
                return handle.read()
        if location and Path(location).resolve().is_relative_to(cache.resolve()):
            return Path(location).read_bytes()
    raise AssertionError("Media has neither embedded bytes nor a trusted HF/cache path")


def audit_case(case: dict, *, require_wheel: bool) -> dict:
    from huggingface_hub import HfApi

    import daft

    module_path = Path(daft.__file__).resolve()
    direct_url = json.loads(distribution("daft").read_text("direct_url.json") or "{}")
    if require_wheel and (
        module_path.is_relative_to(ROOT / "daft") or direct_url.get("dir_info", {}).get("editable", False)
    ):
        raise AssertionError(f"Release gate imported the checkout, not the candidate wheel: {module_path}")
    revision = case["kwargs"]["revision"]
    info = HfApi(token=False).dataset_info(case["repo"], revision=revision, files_metadata=True)
    if info.sha != revision:
        raise AssertionError("HF did not resolve the pinned revision")
    files = info.siblings
    if case["kwargs"]["format"] == "webdataset" and case["kwargs"].get("data_dir"):
        prefix = case["kwargs"]["data_dir"].rstrip("/") + "/"
        files = [file for file in files if file.rfilename.startswith(prefix)]
    sizes = [file.size for file in files]
    if not sizes:
        raise AssertionError("Byte-budget selection matched no files")
    if any(size is None for size in sizes) or sum(sizes) > MAX_REPOSITORY_BYTES:
        raise AssertionError("Read selection exceeds the 50 MB preflight budget or has unknown file sizes")
    # Bound the whole repo for eager fallback; an explicitly selected native TAR
    # directory can be bounded by its own immutable file metadata.
    if case["category"] == "webdataset":
        daft.set_execution_config(scantask_max_parallel=1)
    config = daft.IOConfig(
        hf=daft.io.HuggingFaceConfig(anonymous=True),
        http=daft.io.HTTPConfig(num_tries=2, connect_timeout_ms=10_000, read_timeout_ms=30_000),
    )
    frame = daft.read_huggingface(case["repo"], io_config=config, **case["kwargs"])
    required = case.get("columns", [])
    if not set(required).issubset(frame.column_names) or not frame.column_names:
        raise AssertionError(f"Required columns {required} missing from {frame.column_names}")
    rows = frame.limit(3).to_pylist()
    if not rows or len(rows) > 3:
        raise AssertionError("Expected a nonempty bounded result")
    first = required[0] if required else frame.column_names[0]
    projected = frame.select(first).where(daft.col(first).not_null()).limit(3).to_pydict()
    if set(projected) != {first} or not projected[first]:
        raise AssertionError("Projection/filter/limit returned no non-null values")
    media = case.get("media")
    if media:
        field = media["column"]
        value = next((row[field] for row in rows if row.get(field) is not None), None)
        payload = _media_bytes(value, Path(os.environ["HF_HOME"]))
        if media["kind"] == "image":
            from PIL import Image

            with Image.open(io.BytesIO(payload)) as image:
                image.load()
                if min(image.size) <= 0:
                    raise AssertionError("Invalid image dimensions")
        else:
            import soundfile

            with soundfile.SoundFile(io.BytesIO(payload)) as audio:
                if audio.frames <= 0 or audio.samplerate <= 0:
                    raise AssertionError("Invalid audio samples")
    if case.get("payload_column") and not _media_bytes(rows[0][case["payload_column"]], Path(os.environ["HF_HOME"])):
        raise AssertionError("Lazy file payload is empty")
    return {
        "id": case["id"],
        "category": case["category"],
        "status": "passed",
        "revision": revision,
        "repo_bytes": sum(sizes),
        "rows": len(rows),
        "schema": str(frame.schema()),
        "daft_module": str(module_path),
        "daft_version": daft.__version__,
        "wheel_sha256": direct_url.get("archive_info", {}).get("hashes", {}).get("sha256"),
        "webdataset_parallelism": 1 if case["category"] == "webdataset" else None,
    }


def run(args: argparse.Namespace) -> int:
    cases = load_catalog()
    if args.validate:
        print(f"Validated {len(cases)} pinned cases; five per format")
        return 0
    if args.worker:
        case = next(case for case in cases if case["id"] == args.worker)
        result = _retry(lambda: audit_case(case, require_wheel=args.require_wheel))
        args.result.write_text(json.dumps(result, indent=2))
        return 0
    selected = [case for case in cases if not args.category or case["category"] == args.category]
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="daft-hf-audit-") as task_dir:
        env = {
            **os.environ,
            "DAFT_RUNNER": "native",
            "DAFT_ANALYTICS_ENABLED": "0",
            "DAFT_PROGRESS_BAR": "0",
            "HF_HOME": str(Path(task_dir) / "cache"),
            "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1",
        }
        # Isolate from the checkout and eliminate inherited credential/cache overrides.
        for key in ("PYTHONPATH", "HF_TOKEN", "HF_HUB_CACHE", "HF_DATASETS_CACHE", "HUGGING_FACE_HUB_TOKEN"):
            env.pop(key, None)
        for case in selected:
            started = time.monotonic()
            result_path = args.output.resolve() / f"{case['id']}.json"
            command = [
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                "--worker",
                case["id"],
                "--result",
                str(result_path),
            ]
            if args.require_wheel:
                command.append("--require-wheel")
            try:
                completed = subprocess.run(
                    command, cwd=task_dir, env=env, capture_output=True, text=True, timeout=args.timeout, check=False
                )
                (args.output / f"{case['id']}.log").write_text(completed.stdout + completed.stderr)
                if completed.returncode != 0:
                    result = {
                        "id": case["id"],
                        "category": case["category"],
                        "status": "failed",
                        "exit_code": completed.returncode,
                    }
                else:
                    result = json.loads(result_path.read_text())
            except subprocess.TimeoutExpired as error:
                result = {"id": case["id"], "category": case["category"], "status": "timeout"}
                (args.output / f"{case['id']}.log").write_text(str(error))
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
            results.append(result)
            print(f"{case['id']}: {result['status']} ({result['elapsed_seconds']}s)", flush=True)
    report = {"results": results, "full_catalog": not args.category, "require_wheel": args.require_wheel}
    (args.output / "report.json").write_text(json.dumps(report, indent=2))
    return int(any(result["status"] != "passed" for result in results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Release-only Hugging Face dataset audit")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--category", choices=sorted(FORMATS))
    parser.add_argument("--require-wheel", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("hf-audit-results"))
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    sys.exit(run(parser.parse_args()))
