"""Memory monitor subprocess.

Polls a target process's RSS every POLL_INTERVAL seconds and writes
(timestamp, rss_gb) rows to a CSV until the process exits.

Usage:
    python monitor.py <pid> <output_csv_path>
"""

from __future__ import annotations

import csv
import sys
import time

import psutil

POLL_INTERVAL = 0.5  # seconds


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python monitor.py <pid> <output_csv_path>")
        sys.exit(1)

    pid = int(sys.argv[1])
    output_path = sys.argv[2]

    process = psutil.Process(pid)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "rss_gb"])
        f.flush()

        while True:
            try:
                mem = process.memory_info()
                writer.writerow([time.time(), mem.rss / (1024**3)])
                f.flush()
                time.sleep(POLL_INTERVAL)
            except psutil.NoSuchProcess:
                break


if __name__ == "__main__":
    main()
