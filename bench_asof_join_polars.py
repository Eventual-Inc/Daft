from __future__ import annotations

import os
import threading
import time

import polars as pl
import psutil

frames = pl.read_parquet("/Users/euanlimzx/Desktop/Daft/data/asof_join_aligned/frames")
telemetry = pl.read_parquet("/Users/euanlimzx/Desktop/Daft/data/asof_join_aligned/telemetry")

proc = psutil.Process(os.getpid())
peak_uss = 0
stop_monitor = threading.Event()


def monitor_uss():
    global peak_uss
    while not stop_monitor.is_set():
        uss = proc.memory_full_info().uss
        if uss > peak_uss:
            peak_uss = uss
        time.sleep(0.05)


monitor_thread = threading.Thread(target=monitor_uss, daemon=True)
monitor_thread.start()

start = time.perf_counter()
result = frames.join_asof(telemetry, on="ts_ns", strategy="backward")
elapsed = time.perf_counter() - start

stop_monitor.set()
monitor_thread.join()

print(f"rows:      {len(result)}")
print(f"wall time: {elapsed:.3f}s")
print(f"peak USS:  {peak_uss / 1024**2:.1f} MB")
