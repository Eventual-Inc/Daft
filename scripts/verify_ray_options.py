from __future__ import annotations

import daft
from daft.daft import ResourceRequest
from daft.udf.legacy import udf as legacy_udf
from daft.runners.ray_runner import _get_ray_task_options


def main() -> None:
    # Verify ray_options -> resource_request.label_selector
    ls = {"group": "g1"}

    @legacy_udf(return_dtype=daft.DataType.string(), ray_options={"label_selector": ls})
    def f(x):
        return x

    assert f.resource_request is not None
    assert f.resource_request.label_selector == ls
    print("UDF ray_options propagated to ResourceRequest.label_selector: OK")

    # Verify building Ray options includes label_selector
    rr = ResourceRequest(label_selector={"k": "v"})
    opts = _get_ray_task_options(rr)
    assert "label_selector" in opts and opts["label_selector"] == {"k": "v"}
    print("Ray task options include label_selector: OK")

    # Optional: run a tiny ray job to ensure basic flow works without labels
    daft.set_runner_ray()
    df = daft.from_pydict({"data": ["a", "b"]})

    @legacy_udf(return_dtype=daft.DataType.string(), ray_options={"label_selector": {}})
    def echo(x):
        return x

    df = df.with_column("out", echo(df["data"]))
    # Just trigger execution
    df.collect()
    print("Ray runner basic execution with empty label_selector: OK")


if __name__ == "__main__":
    main()
