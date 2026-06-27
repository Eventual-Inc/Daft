"""Benchmark AudioFile/ImageFile method behavior with and without Daft buffering.

The benchmark compares the same method bodies using:

- ``none``: ``buffer_size=None``
- ``default``: the current Daft constants used by the file methods

It can run against local files and against a local HTTP server with Range
support. The HTTP mode is useful because remote object access is where Daft's
read buffering should matter most.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import dataclasses
import http
import mimetypes
import os
import pathlib
import statistics
import tempfile
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import daft
from daft.dependencies import librosa, np, pil_image, sf
from daft.file.file import BUFFER_COPY, BUFFER_METADATA

Operation = Callable[[], Any]


@dataclasses.dataclass
class HttpStats:
    requests: int = 0
    head_requests: int = 0
    get_requests: int = 0
    range_requests: int = 0
    bytes_sent: int = 0

    def reset(self) -> None:
        self.requests = 0
        self.head_requests = 0
        self.get_requests = 0
        self.range_requests = 0
        self.bytes_sent = 0


@dataclasses.dataclass
class BenchmarkRow:
    source: str
    file_kind: str
    method: str
    variant: str
    buffer_size: str
    runs: int
    median_ms: float
    mean_ms: float
    min_ms: float
    max_ms: float
    result: str
    http_requests_median: float = 0.0
    http_range_requests_median: float = 0.0
    http_bytes_sent_median: float = 0.0


class RangeRequestHandler(BaseHTTPRequestHandler):
    root: pathlib.Path
    stats: HttpStats
    lock: threading.Lock

    server_version = "DaftMediaBenchHTTP/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_HEAD(self) -> None:
        self._serve(send_body=False)

    def do_GET(self) -> None:
        self._serve(send_body=True)

    def _serve(self, *, send_body: bool) -> None:
        with self.lock:
            self.stats.requests += 1
            if self.command == "HEAD":
                self.stats.head_requests += 1
            elif self.command == "GET":
                self.stats.get_requests += 1

        try:
            path = self._resolve_path()
        except ValueError:
            self.send_error(http.HTTPStatus.NOT_FOUND)
            return

        if not path.exists() or not path.is_file():
            self.send_error(http.HTTPStatus.NOT_FOUND)
            return

        size = path.stat().st_size
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        range_header = self.headers.get("Range")
        start = 0
        end = size - 1
        partial = False

        if range_header:
            maybe_range = self._parse_range(range_header, size)
            if maybe_range is None:
                self.send_response(http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.end_headers()
                return
            start, end = maybe_range
            partial = True

        body_len = max(end - start + 1, 0)
        self.send_response(http.HTTPStatus.PARTIAL_CONTENT if partial else http.HTTPStatus.OK)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(body_len))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()

        if partial:
            with self.lock:
                self.stats.range_requests += 1

        if not send_body or body_len == 0:
            return

        with path.open("rb") as fh:
            fh.seek(start)
            remaining = body_len
            while remaining > 0:
                chunk = fh.read(min(remaining, 1024 * 1024))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)
                with self.lock:
                    self.stats.bytes_sent += len(chunk)

    def _resolve_path(self) -> pathlib.Path:
        raw_path = urllib.parse.urlsplit(self.path).path
        relative = urllib.parse.unquote(raw_path).lstrip("/")
        candidate = (self.root / relative).resolve()
        root = self.root.resolve()
        if root != candidate and root not in candidate.parents:
            raise ValueError("path escapes server root")
        return candidate

    @staticmethod
    def _parse_range(range_header: str, size: int) -> tuple[int, int] | None:
        if not range_header.startswith("bytes="):
            return None
        spec = range_header.removeprefix("bytes=").split(",", 1)[0].strip()
        if "-" not in spec:
            return None
        start_s, end_s = spec.split("-", 1)
        if start_s == "":
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(size - suffix, 0)
            end = size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else size - 1
        if start >= size or start < 0 or end < start:
            return None
        return start, min(end, size - 1)


@contextlib.contextmanager
def local_http_server(root: pathlib.Path) -> Iterator[tuple[str, HttpStats]]:
    stats = HttpStats()
    lock = threading.Lock()

    class Handler(RangeRequestHandler):
        pass

    Handler.root = root
    Handler.stats = stats
    Handler.lock = lock

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", stats
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def create_sample_files(root: pathlib.Path, image_size: int, audio_seconds: float, sample_rate: int) -> tuple[pathlib.Path, pathlib.Path]:
    root.mkdir(parents=True, exist_ok=True)
    image_path = root / f"image_{image_size}.jpg"
    audio_path = root / f"audio_{int(audio_seconds)}s.wav"

    if not image_path.exists():
        rng = np.random.default_rng(0)
        image = rng.integers(0, 256, size=(image_size, image_size, 3), dtype=np.uint8)
        pil_image.fromarray(image, mode="RGB").save(image_path, quality=92)

    if not audio_path.exists():
        t = np.linspace(0, audio_seconds, int(sample_rate * audio_seconds), endpoint=False)
        left = 0.2 * np.sin(2 * np.pi * 220 * t)
        right = 0.2 * np.sin(2 * np.pi * 440 * t)
        audio = np.stack([left, right], axis=1).astype(np.float32)
        sf.write(str(audio_path), audio, sample_rate)

    return image_path, audio_path


def summarize_value(value: Any) -> str:
    if isinstance(value, dict):
        return ",".join(f"{key}={value[key]}" for key in sorted(value))
    if hasattr(value, "shape"):
        return f"shape={value.shape}"
    if hasattr(value, "size") and hasattr(value, "mode"):
        return f"size={value.size},mode={value.mode}"
    return type(value).__name__


def image_metadata(file: daft.ImageFile, buffer_size: int | None) -> dict[str, Any]:
    with file.open(buffer_size=buffer_size) as fh:
        img = pil_image.open(fh)
        return {"width": img.width, "height": img.height, "format": img.format, "mode": img.mode}


def image_decode(file: daft.ImageFile, buffer_size: int | None) -> Any:
    with file.open(buffer_size=buffer_size) as fh:
        img = pil_image.open(fh)
        img.load()
        return img


def audio_metadata(file: daft.AudioFile, buffer_size: int | None) -> dict[str, Any]:
    with file.open(buffer_size=buffer_size) as fh, sf.SoundFile(fh) as audio:
        return {
            "sample_rate": audio.samplerate,
            "channels": audio.channels,
            "frames": audio.frames,
            "format": audio.format,
            "subtype": audio.subtype,
        }


def audio_to_numpy(file: daft.AudioFile, copy_buffer_size: int | None) -> Any:
    with file.to_tempfile(copy_buffer_size) as tmp:
        audio, _ = sf.read(tmp)
        return audio


def audio_resample(file: daft.AudioFile, copy_buffer_size: int | None, sample_rate: int) -> Any:
    with file.to_tempfile(copy_buffer_size) as tmp:
        data, original_sample_rate = sf.read(tmp)
        if original_sample_rate == sample_rate:
            return data
        return librosa.resample(data, orig_sr=original_sample_rate, target_sr=sample_rate)


def make_operations(
    image_file: daft.ImageFile,
    audio_file: daft.AudioFile,
    *,
    include_resample: bool,
    resample_rate: int,
) -> list[tuple[str, str, str, str, Operation]]:
    operations: list[tuple[str, str, str, str, Operation]] = [
        (
            "image",
            "metadata",
            "none",
            "None",
            lambda: image_metadata(image_file, None),
        ),
        (
            "image",
            "metadata",
            "default",
            str(BUFFER_METADATA),
            lambda: image_metadata(image_file, BUFFER_METADATA),
        ),
        (
            "image",
            "decode",
            "none",
            "None",
            lambda: image_decode(image_file, None),
        ),
        (
            "image",
            "decode",
            "default",
            str(BUFFER_COPY),
            lambda: image_decode(image_file, BUFFER_COPY),
        ),
        (
            "audio",
            "metadata",
            "none",
            "None",
            lambda: audio_metadata(audio_file, None),
        ),
        (
            "audio",
            "metadata",
            "default",
            str(BUFFER_METADATA),
            lambda: audio_metadata(audio_file, BUFFER_METADATA),
        ),
        (
            "audio",
            "to_numpy",
            "none",
            "None",
            lambda: audio_to_numpy(audio_file, None),
        ),
        (
            "audio",
            "to_numpy",
            "default",
            str(BUFFER_COPY),
            lambda: audio_to_numpy(audio_file, BUFFER_COPY),
        ),
    ]

    if include_resample:
        operations.extend(
            [
                (
                    "audio",
                    "resample",
                    "none",
                    "None",
                    lambda: audio_resample(audio_file, None, resample_rate),
                ),
                (
                    "audio",
                    "resample",
                    "default",
                    str(BUFFER_COPY),
                    lambda: audio_resample(audio_file, BUFFER_COPY, resample_rate),
                ),
            ]
        )

    return operations


def run_operation(
    source: str,
    file_kind: str,
    method: str,
    variant: str,
    buffer_size: str,
    operation: Operation,
    *,
    runs: int,
    warmups: int,
    http_stats: HttpStats | None,
) -> BenchmarkRow:
    for _ in range(warmups):
        operation()

    elapsed_ms: list[float] = []
    requests: list[int] = []
    range_requests: list[int] = []
    bytes_sent: list[int] = []
    result = ""

    for _ in range(runs):
        if http_stats is not None:
            http_stats.reset()
        start = time.perf_counter()
        value = operation()
        elapsed_ms.append((time.perf_counter() - start) * 1000.0)
        result = summarize_value(value)
        if http_stats is not None:
            requests.append(http_stats.requests)
            range_requests.append(http_stats.range_requests)
            bytes_sent.append(http_stats.bytes_sent)

    return BenchmarkRow(
        source=source,
        file_kind=file_kind,
        method=method,
        variant=variant,
        buffer_size=buffer_size,
        runs=runs,
        median_ms=statistics.median(elapsed_ms),
        mean_ms=statistics.fmean(elapsed_ms),
        min_ms=min(elapsed_ms),
        max_ms=max(elapsed_ms),
        result=result,
        http_requests_median=statistics.median(requests) if requests else 0.0,
        http_range_requests_median=statistics.median(range_requests) if range_requests else 0.0,
        http_bytes_sent_median=statistics.median(bytes_sent) if bytes_sent else 0.0,
    )


def run_source(
    source: str,
    image_url: str,
    audio_url: str,
    *,
    runs: int,
    warmups: int,
    include_resample: bool,
    resample_rate: int,
    http_stats: HttpStats | None,
) -> list[BenchmarkRow]:
    image_file = daft.ImageFile(image_url)
    audio_file = daft.AudioFile(audio_url)
    rows: list[BenchmarkRow] = []
    for file_kind, method, variant, buffer_size, operation in make_operations(
        image_file,
        audio_file,
        include_resample=include_resample,
        resample_rate=resample_rate,
    ):
        row = run_operation(
            source,
            file_kind,
            method,
            variant,
            buffer_size,
            operation,
            runs=runs,
            warmups=warmups,
            http_stats=http_stats,
        )
        rows.append(row)
        print_row(row)
    return rows


def print_row(row: BenchmarkRow) -> None:
    extra = ""
    if row.source == "http":
        extra = (
            f" requests={row.http_requests_median:.0f}"
            f" ranges={row.http_range_requests_median:.0f}"
            f" bytes={int(row.http_bytes_sent_median)}"
        )
    print(
        f"{row.source:5} {row.file_kind:5} {row.method:9} {row.variant:15}"
        f" median={row.median_ms:8.3f}ms mean={row.mean_ms:8.3f}ms{extra}"
        f" result={row.result}",
        flush=True,
    )


def write_csv(path: pathlib.Path, rows: list[BenchmarkRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[field.name for field in dataclasses.fields(BenchmarkRow)])
        writer.writeheader()
        for row in rows:
            writer.writerow(dataclasses.asdict(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=pathlib.Path, default=pathlib.Path(".tmp/media-buffer-bench"))
    parser.add_argument("--image-path", type=pathlib.Path)
    parser.add_argument("--audio-path", type=pathlib.Path)
    parser.add_argument("--image-size", type=int, default=2048)
    parser.add_argument("--audio-seconds", type=float, default=30.0)
    parser.add_argument("--sample-rate", type=int, default=48_000)
    parser.add_argument("--sources", default="local,http", help="Comma-separated: local,http")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--include-resample", action="store_true")
    parser.add_argument("--resample-rate", type=int, default=16_000)
    parser.add_argument("--output-csv", type=pathlib.Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not pil_image.module_available():
        raise ImportError("Pillow is required for image benchmarks")
    if not sf.module_available():
        raise ImportError("soundfile is required for audio benchmarks")
    if args.include_resample and not librosa.module_available():
        raise ImportError("librosa is required for --include-resample")

    with tempfile.TemporaryDirectory(prefix="daft_media_bench_") as temp_root:
        sample_root = args.work_dir if args.work_dir else pathlib.Path(temp_root)
        image_path, audio_path = create_sample_files(sample_root, args.image_size, args.audio_seconds, args.sample_rate)
        if args.image_path is not None:
            image_path = args.image_path
        if args.audio_path is not None:
            audio_path = args.audio_path

        print(f"Image: {image_path} ({os.path.getsize(image_path)} bytes)", flush=True)
        print(f"Audio: {audio_path} ({os.path.getsize(audio_path)} bytes)", flush=True)
        print(f"BUFFER_METADATA={BUFFER_METADATA} BUFFER_COPY={BUFFER_COPY}", flush=True)

        rows: list[BenchmarkRow] = []
        sources = {part.strip() for part in args.sources.split(",") if part.strip()}
        unknown = sources - {"local", "http"}
        if unknown:
            raise ValueError(f"Unknown sources: {sorted(unknown)}")

        if "local" in sources:
            rows.extend(
                run_source(
                    "local",
                    str(image_path),
                    str(audio_path),
                    runs=args.runs,
                    warmups=args.warmups,
                    include_resample=args.include_resample,
                    resample_rate=args.resample_rate,
                    http_stats=None,
                )
            )

        if "http" in sources:
            if image_path.parent != audio_path.parent:
                raise ValueError("HTTP source requires image and audio files to live in the same directory")
            with local_http_server(image_path.parent) as (base_url, stats):
                rows.extend(
                    run_source(
                        "http",
                        f"{base_url}/{image_path.name}",
                        f"{base_url}/{audio_path.name}",
                        runs=args.runs,
                        warmups=args.warmups,
                        include_resample=args.include_resample,
                        resample_rate=args.resample_rate,
                        http_stats=stats,
                    )
                )

        if args.output_csv is not None:
            write_csv(args.output_csv, rows)
            print(f"Wrote {args.output_csv}", flush=True)


if __name__ == "__main__":
    main()
