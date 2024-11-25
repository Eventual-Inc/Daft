from pathlib import Path

import daft

df = daft.from_pydict({"nums": [1, 2, 3]})
df = df.with_column("result", daft.col("nums").cbrt()).collect()
df.show()

path = Path("/tmp/ray/session_latest/logs/daft")
path.mkdir(parents=True, exist_ok=True)

with open(path / "metrics.csv") as f:
    f.write("1,2,3")
