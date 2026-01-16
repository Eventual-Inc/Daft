import daft
from daft.daft import dashboard
import time
import os

def main():
    # 启动 Dashboard
    print("Launching Daft Dashboard (Ray Mode)...")
    handle = dashboard.launch(noop_if_initialized=True)
    port = handle.get_port()
    dashboard_url = f"http://127.0.0.1:{port}"
    os.environ["DAFT_DASHBOARD_URL"] = dashboard_url
    
    print(f"\nDashboard launched at: {dashboard_url}")
    print("Use the 'Open Preview' tool or access the URL in your browser.\n")

    # 设置 Ray Runner
    print("Connecting to Ray cluster...")
    daft.set_runner_ray(address="auto")

    print("Running a sample Daft query on Ray...")
    
    # repartition(8) 触发 shuffle
    df = daft.from_pydict({"a": list(range(10000))})
    df = df.with_column("b", df["a"] * 2)
    df = df.repartition(8)
    df = df.with_column("c", df["b"] + 1)
    
    result = df.collect()
    print(f"Query completed. Result count: {len(result)}")
    
    print("\nDashboard is still running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Dashboard...")
        handle.shutdown(noop_if_shutdown=True)

if __name__ == "__main__":
    main()
