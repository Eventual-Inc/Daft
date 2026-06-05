from __future__ import annotations

import os

os.environ.setdefault("DAFT_RUNNER", "native")

import threading
import time

import psutil

import daft

frames = daft.read_parquet(
    "/Users/euanlimzx/Desktop/Daft/data/asof_join_aligned/frames",
    _multithreaded_io=False,
)

telemetry = daft.read_parquet(
    "/Users/euanlimzx/Desktop/Daft/data/asof_join_aligned/telemetry",
    _multithreaded_io=False,
)

result = frames.join_asof(
    telemetry,
    on="ts_ns",
    _assume_sorted_and_aligned=True,
)

proc = psutil.Process(os.getpid())
peak_rss = 0
stop_monitor = threading.Event()


def monitor_rss():
    global peak_rss
    while not stop_monitor.is_set():
        rss = proc.memory_full_info().uss
        if rss > peak_rss:
            peak_rss = rss
        time.sleep(0.05)


monitor_thread = threading.Thread(target=monitor_rss, daemon=True)
monitor_thread.start()

start = time.perf_counter()
result.collect()
elapsed = time.perf_counter() - start

stop_monitor.set()
monitor_thread.join()

print(f"wall time: {elapsed:.3f}s")
print(f"peak USS:  {peak_rss / 1024**2:.1f} MB")
