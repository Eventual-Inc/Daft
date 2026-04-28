# Hello C++ Native Extension

A minimal Daft native extension written in pure C++ using the [Apache Arrow C++ library](https://arrow.apache.org/docs/cpp/).

This is the C++ counterpart of the [hello](../hello/) Rust example. It implements a single `greet_cpp` scalar function that prepends `"Hello, "` to every string in a column.

## Prerequisites

- CMake >= 3.20
- A C++17 compiler
- Apache Arrow C++ (with headers)

### Install Arrow C++

**macOS (Homebrew):**

```bash
brew install apache-arrow
```

**Ubuntu / Debian:**

```bash
sudo apt install -y libarrow-dev
```

**Conda:**

```bash
conda install -c conda-forge arrow-cpp
```

## Development

### Build the shared library

```bash
cmake -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build
```

The `CMAKE_EXPORT_COMPILE_COMMANDS` flag generates `build/compile_commands.json` for clangd / LSP support. Symlink it to the project root if your editor needs it:

```bash
ln -sf build/compile_commands.json .
```

### Install as a Python package

```bash
uv pip install -e .
```

This uses [scikit-build-core](https://scikit-build-core.readthedocs.io/) to build the C++ library and install it into the Python package.

### Run tests

```bash
pytest -v tests/
```

## Usage

```python
import daft
import hello_cpp
from hello_cpp import greet
from daft import col
from daft.session import Session

sess = Session()
sess.load_extension(hello_cpp)

df = daft.from_pydict({"name": ["John", "Paul", "George", None]})

with sess:
    df.select(greet(col("name"))).show()

# ╭──────────────────╮
# │ greet_cpp         │
# │ ---               │
# │ Utf8              │
# ╞══════════════════╡
# │ Hello, John!      │
# ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
# │ Hello, Paul!      │
# ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
# │ Hello, George!    │
# ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
# │ None              │
# ╰──────────────────╯
```
