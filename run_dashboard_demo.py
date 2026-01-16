import daft
from daft.daft import dashboard
import time
import os

def main():
    # 启动 Dashboard
    print("Launching Daft Dashboard...")
    handle = dashboard.launch(noop_if_initialized=True)
    port = handle.get_port()
    dashboard_url = f"http://127.0.0.1:{port}"
    os.environ["DAFT_DASHBOARD_URL"] = dashboard_url
    
    print(f"\nDashboard launched at: {dashboard_url}")
    print("Use the 'Open Preview' tool or access the URL in your browser.\n")

    # 设置 Ray Runner (如果需要分布式执行)
    # daft.set_runner_ray(address="auto")
    # 或者使用 Native Runner
    daft.set_runner_native()

    print("Running a sample Daft query...")
    
    # 创建一个简单的数据处理流程，让 Dashboard 有内容显示
    df = daft.from_pydict({"a": list(range(1000))})
    df = df.with_column("b", df["a"] * 2)
    df = df.repartition(4)
    
    # 为了能有足够时间观察 Dashboard，我们可以多次运行或者sleep
    # 这里我们运行一次 collect
    result = df.collect()
    print(f"Query completed. Result count: {len(result)}")
    
    print("\nDashboard is still running. Press Ctrl+C to stop.")
    try:
        # 保持进程运行以便 Dashboard 继续服务
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Dashboard...")
        handle.shutdown(noop_if_shutdown=True)

if __name__ == "__main__":
    main()
