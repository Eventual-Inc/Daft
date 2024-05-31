from __future__ import annotations

import io
import shutil
import signal
import subprocess as sp
import time

# extracted from aioboto3
#    https://github.com/terrycain/aioboto3/blob/16a1a1085191ebe6d40ee45d9588b2173738af0c/tests/mock_server.py
import pytest
import requests

_proxy_bypass = {
    "http": None,
    "https": None,
}


def start_service(host: str, port: int, log_file: io.IOBase):
    moto_svr_path = shutil.which("moto_server")
    if not moto_svr_path:
        pytest.skip("moto not installed")
    args = [moto_svr_path, "-H", host, "-p", str(port)]
    process = sp.Popen(args, stdout=log_file, stderr=sp.STDOUT, shell=True)
    # Use static data fetch as healthcheck API.
    url = f"http://{host}:{port}/moto-api/data.json"

    for _ in range(0, 100):
        output = process.poll()
        if output is not None:
            print(f"moto_server exited status {output}")
            pytest.fail("Cannot start mock AWS server")

        try:
            # we need to bypass the proxies due to monkeypatches
            r = requests.get(url, timeout=5, proxies=_proxy_bypass)
            if r.status_code == 200:
                break
        except requests.exceptions.ConnectionError:
            pass

        time.sleep(0.1)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail("Cannot start mock AWS server")

    return process


def stop_process(process):
    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        outs, errors = process.communicate(timeout=20)
        exit_code = process.returncode
        msg = f"Child process finished {exit_code} in unclean way:\n{outs}\n\n{errors}"
        raise RuntimeError(msg)
