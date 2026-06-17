from __future__ import annotations

import daft

daft.set_runner_ray()

df = daft.from_pydict({"x": list(range(100))}).into_partitions(4)
result = df.sample(size=10, seed=42).collect()

result.show()
print("rows:", result.count_rows())
