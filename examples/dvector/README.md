# dvector

Vector distance functions for [Daft](https://docs.daft.ai), implemented as a native extension.

## Functions

| Function                 | Description                                     | Input           | Output  |
|--------------------------|-------------------------------------------------|-----------------|---------|
| `l2_distance(a, b)`      | Euclidean distance                              | float vectors   | Float64 |
| `inner_product(a, b)`    | Negative inner product (pgvector convention)    | float vectors   | Float64 |
| `cosine_distance(a, b)`  | Cosine distance (null for zero-norm)            | float vectors   | Float64 |
| `l1_distance(a, b)`      | Manhattan distance                              | float vectors   | Float64 |
| `hamming_distance(a, b)` | Count of differing positions                    | boolean vectors | UInt32  |
| `jaccard_distance(a, b)` | 1 - intersection/union (null if union is empty) | boolean vectors | Float64 |

Vectors can be FixedSizeList, List, or LargeList columns. Float vectors support Float32 and Float64 elements.

## Install

```sh
```

Requires a Rust toolchain and `setuptools-rust`.

## Usage

```python
import daft
import dvector
from dvector import l2_distance

# Load the extension into the active session
daft.load_extension(dvector)

df = daft.from_pydict({
    "a": [[1.0, 2.0, 3.0], [0.0, 0.0, 0.0]],
    "b": [[4.0, 5.0, 6.0], [1.0, 1.0, 1.0]],
})

df.select(l2_distance(daft.col("a"), daft.col("b"))).collect()
# ╭───────────╮
# │ result    │
# │ ---       │
# │ Float64   │
# ╞═══════════╡
# │ 5.196152  │
# ├╌╌╌╌╌╌╌╌╌╌╌┤
# │ 1.732051  │
# ╰───────────╯
```

## Development

```sh
# Build and install with the native library
uv pip install -e .

# Run tests
pytest -v tests/
```
