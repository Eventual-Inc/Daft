## Summary
Fixes #5696. Unblocks #5691.

When using `prompt()` with `return_format=PydanticModel` in Google Colab, cloudpickle fails with:
```
TypeError: self.handle cannot be converted to a Python object for pickling
```

### Root Cause
When a Pydantic model is defined in a Colab notebook cell, Python's class creation machinery captures references to the notebook's execution frame globals. These globals include IPython/ZMQ internals (socket handles) that cannot be pickled.

### Solution
Intercept Pydantic model classes at the **pickle layer** using a custom `ColabSafePickler`. When running in Google Colab, `daft.pickle.dumps()` uses this custom pickler which:
1. Detects Pydantic model classes via `reducer_override()`
2. Recreates them using `pydantic.create_model()` with a clean namespace
3. Serializes the cleaned model instead of the polluted original

Key changes:
- Add `daft/pickle/_colab_compat.py` module with:
  - `_is_colab()` detection (cached at module load as `IS_COLAB`)
  - `clean_pydantic_model()` to recursively recreate models
  - `colab_safe_dumps()` with custom `ColabSafePickler`
- Modify `daft/pickle/pickle.py` to use `colab_safe_dumps()` when in Colab

This approach provides **broader coverage** than fixing individual call sites - it handles:
- `prompt()` with `return_format=PydanticModel`
- Functions that reference Pydantic models (in type annotations or function body) when serialized
- Any other serialization path that uses `daft.pickle.dumps()`

Note: The cleaned model preserves field names, types, and defaults, which is sufficient for JSON schema extraction and model reconstruction.

### Supported Cases
- Simple Pydantic models
- Nested models (`Parent` with `list[Child]`)
- Self-referential models (both `list[TreeNode] | None` and direct `child: TreeNode | None`)
- Models deeply nested in dicts/lists
- Functions with Pydantic return type annotations

## Test Plan

### Unit Tests (`tests/ai/test_colab_compat.py`)
- [x] `clean_pydantic_model()`: Simple, nested, self-referential models, defaults preservation, caching
- [x] `colab_safe_dumps()`: Nested/deeply-nested models, non-Pydantic objects preserved, functions with Pydantic types

### Manual Colab Validation
- [x] Standalone validation in [this Colab notebook](https://colab.research.google.com/drive/1nlh75E_2zYL0gxqRthDshlDRK4tvYLZU) demonstrating the original failing case and fix
- [x] End-to-end validation in Google Colab with custom-built wheel (see instructions below)

### Colab End-to-End Testing (Custom Wheel)

Since Daft has Rust dependencies that can't be easily built in Colab, full end-to-end testing requires building a Linux x86_64 wheel locally and uploading it:

#### 1. Build Linux wheel (on macOS with Docker + QEMU)
```bash
# Build dashboard frontend first
cd src/daft-dashboard/frontend && bun install && bun run build && cd -

# Build Linux x86_64 wheel using maturin in Docker
docker run --rm --platform linux/amd64 \
  -e GITHUB_ACTIONS=true \
  -v $(pwd):/io -w /io \
  ghcr.io/pyo3/maturin:v1.7.4 \
  build --release --out /io/dist-linux
```

#### 2. Upload and install in Colab
```python
from google.colab import files
uploaded = files.upload()  # Upload the .whl file
!pip install daft-*.whl
```

#### 3. Test the fix
```python
import daft
from daft.functions import prompt
from pydantic import BaseModel

class Result(BaseModel):
    answer: bool

df = daft.from_pydict({"text": ["hello", "world"]})
df = df.with_column("upper", df["text"].upper())
df.show(2)

import openai
try:
    df = df.with_column("analysis", prompt(["Analyze:", df["text"]], model="gpt-4o-mini", provider="openai", return_format=Result))
except openai.OpenAIError:
    pass  # No API key, that's fine

print("Serialization worked!")
```

This confirms the fix works in the actual Colab environment where the original issue occurs - the Pydantic model can now be serialized without the `TypeError: self.handle cannot be converted to a Python object for pickling` error.
