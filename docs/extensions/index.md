# Daft Extensions

!!! warning "Experimental"

    Native extensions are experimental and may change in future releases.

> Please see the [prompt](#prompt) if you want help generating an extension.

This document is a guide for authoring Daft native extensions in Rust.
Daft supports native Rust extensions by leveraging a stable C ABI and Arrow FFI. Today we support authoring native
scalar functions, but are actively working on additional native extension features.

## Example

This example shows the end-result of our 'hello' extension with a native 'greet' scalar function.

```python
import daft

# Step 1. Import your extension module
import hello

# Step 2. Load the extension into the current daft session
daft.load_extension(hello)

# Step 3. Use in your dataframe!
df = daft.from_pydict({"name": ["John", "Paul"]})
df = df.select(hello.greet(df["name"]))
df.show()

"""
╭──────────────╮
│ greet        │
│ ---          │
│ String       │
╞══════════════╡
│ Hello, John! │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Hello, Paul! │
╰──────────────╯
"""
```

## Tutorial

### 1. Setup

This section walks through project setup from scratch; You will need a Rust toolchain and Python 3.10+.

!!! note "Note"

    We are actively working on a cloneable template.


```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Now you can create the project scaffolding.

```bash
# Create an empty directory
mkdir hello && cd hello

# Setup rust project
cargo init --lib

# Setup python project (for function signatures)
uv init
```

The crate must compile as a `cdylib` so it can be loaded at runtime via `dlopen`.

```bash
cat Cargo.toml
```

```toml
[workspace]

[package]
name = "hello"
edition = "2024"
version = "0.1.0"

[lib]
name = "hello"
crate-type = ["cdylib"]

[dependencies]
daft-ext = <version>
arrow-array = "57.1.0"
arrow-schema = "57.1.0"
```

!!! tip "Arrow types"

    Use `arrow-array` builders and downcasting directly for working with data.
    The `daft-ext` prelude re-exports common types like `ArrayRef` and `Field`.
    Import `arrow_array::Array` for the `len()` and `is_null()` methods, and
    `arrow_array::cast::AsArray` for downcasting (e.g., `as_string`).

Then update the pyproject to use `setuptools-rust` as the build system.

```bash
cat pyproject.toml
```

```toml
[build-system]
requires = ["setuptools", "setuptools-rust"]
build-backend = "setuptools.build_meta"

[project]
name = "hello"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = ["daft"]

[project.optional-dependencies]
test = ["pytest"]
```


You will need to create a `setup.py` file with the following contents.

!!! note "Note"

    The `RustExtension` entry tells `setuptools-rust` to compile the cdylib and place it inside the Python package directory.
    We use the `Binding::NoBinding` because Daft extensions export a raw C symbol, not PyO3 bindings. The target name `hello.libhello`
    places the compiled `.so` inside the `hello/` package directory so the Session's `load_extension` can find it.

```bash
cat setup.py
```

```python
from setuptools import find_packages, setup
from setuptools_rust import Binding, RustExtension

setup(
    packages=find_packages(),
    rust_extensions=[
        RustExtension(
            "hello.libhello",   # <python_package>.<lib_name>
            path="Cargo.toml",
            binding=Binding.NoBinding,
            strip=True,
        )
    ],
)
```

### 2. Hello, World!

An extension has two parts: a **module** (the entry point) and one or more **scalar functions**.

```bash
cat src/lib.rs
```

```rust
use std::ffi::CStr;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field};
use daft_ext::prelude::*;

// ── Module ──────────────────────────────────────────────────────────

// #[daft_extension] generates the `daft_module_magic` C symbol that Daft's runtime looks for
// when loading the shared library. It converts HelloExtension → hello_extension for the module name.
#[daft_extension]
struct HelloExtension;

impl DaftExtension for HelloExtension {

    /// This is the extension install hook for defining functions in the session.
    /// Called once when the extension is loaded into a session. Register each function here.
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(Greet));
    }
}

// ── Function ────────────────────────────────────────────────────────

/// The function type which is registered as an Arc<dyn DaftScalarFunction>.
struct Greet;

impl DaftScalarFunction for Greet {
    /// Function name used to look it up from Python.
    /// Must be a `&CStr` (use the `c"..."` literal).
    fn name(&self) -> &CStr {
        c"greet"
    }

    /// Type checking.
    /// Given the input `Field` schemas, validate types and return the output `Field`.
    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        if args.len() != 1 {
            return Err(DaftError::TypeError(
                format!("greet: expected 1 argument, got {}", args.len()),
            ));
        }
        if *args[0].data_type() != DataType::Utf8 && *args[0].data_type() != DataType::LargeUtf8 {
            return Err(DaftError::TypeError(
                format!("greet: expected string argument, got {:?}", args[0].data_type()),
            ));
        }
        Ok(Field::new("greet", DataType::Utf8, true))
    }

    /// Evaluation. Receives Arrow arrays, returns an Arrow array. Operates on entire columns at once.
    /// All data flows through Arrow arrays — no per-row Python overhead.
    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        let names = args[0].as_string::<i64>();
        let mut builder = StringBuilder::with_capacity(names.len(), names.len() * 16);
        for i in 0..names.len() {
            if names.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(format!("Hello, {}!", names.value(i)));
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}
```

!!! tip "String types"

    Daft uses `LargeUtf8` (i64 offsets) for strings internally. When downcasting string arrays,
    use `as_string::<i64>()` — using `i32` will panic at runtime. Similarly, when checking types
    in `return_field`, accept `DataType::LargeUtf8`.

!!! tip "Naming"

    Function names are global within a session. Use a prefix (e.g., `myext_greet`) to avoid
    collisions when your extension defines many functions or might be loaded alongside others.

!!! tip "Multiple functions"

    Register as many functions as you need in `install()` — each is an independent struct
    implementing `DaftScalarFunction`.

!!! tip "Errors"

    Return `Err(DaftError::TypeError(...))` for schema violations in `return_field`,
    and `Err(DaftError::RuntimeError(...))` for execution failures in `call`.

Now we define the python symbols for use in the Expression DSL; we link to rust via `daft.get_function`.

!!! note "Note"

    Python sources let you write python function signatures and any argument pre-processing before linking
    to the function symbol. The `daft.get_function` method is calling `get_function` on the active session
    to resolve the given name and arguments to some defined function in the session. Notice how we don't
    technically need python to resolve functions in SQL (hence no PyO3) but these python functions give us
    nice pythonic functions with autocomplete and doc comments when using the Expression DSL.

```bash
cat hello/__init__.py
```

```python
from __future__ import annotations
from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression

def greet(name: Expression) -> Expression:
    """Greet someone by name."""
    return daft.get_function("greet", name)
```

`daft.get_function` looks up a function registered with the current session by the name returned from `DaftScalarFunction::name()`.

Add an empty `hello/py.typed` marker if you want type-checker support.

### 3. Build, Install, Test

```bash
# Compile the Rust cdylib and install the package in editable mode
uv pip install -e .
```

Here are some sanity check tests. Notice how we use a scoped session rather than
the global active session. How you choose to load extensions is up to you, and this
tutorial has covered both.

!!! tip "Session isolation"

    Extensions are loaded once into the process and the session serves as a scoping mechanism
    for name resolution; calling `load_extension` multiple times will only `dlopen` once for
    this process. Functions are only available in sessions where the extension is loaded.
    Use the `with sess:` context manager to scope queries to a specific session.

```bash
cat tests/test_hello.py
```

```python
import daft
import hello
from daft import col
from daft.session import Session
from hello import greet

def test_greet():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["John", "Paul"]})

    with sess:
        result = df.select(greet(col("name"))).collect().to_pydict()

    values = result["greet"]
    assert values[0] == "Hello, John!"
    assert values[1] == "Hello, Paul!"

def test_greet_null():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["George", "Ringo", None]})

    with sess:
        result = df.select(greet(col("name"))).collect().to_pydict()

    values = result["greet"]
    assert values[0] == "Hello, George!"
    assert values[1] == "Hello, Ringo!"
    assert values[2] is None
```

Now run the tests!

```bash
pytest -v tests/
```

## Prompt

You can paste this whole document and prompt into Claude Code to scaffold a Daft extension for you.

````markdown
Create a Daft native extension called `<extension_name>` with the following scalar functions:

<describe each function: name, arguments with types, return type, and behavior>

Follow the Daft extension authoring guide at docs/extensions/index.md. Here is a summary of the key conventions:

## Project structure

```
<extension_name>/
  Cargo.toml           # [lib] crate-type = ["cdylib"], depends on daft-ext, arrow-array, arrow-schema
  pyproject.toml       # build-system: setuptools + setuptools-rust
  setup.py             # RustExtension("<pkg>.lib<pkg>", binding=NoBinding, strip=True)
  <extension_name>/
    __init__.py        # Python wrappers using daft.get_function("name", *args)
    py.typed           # empty PEP 561 marker
  src/
    lib.rs             # #[daft_extension] struct + DaftScalarFunction impls
  tests/
    test_<name>.py     # pytest tests using Session fixture
  .gitignore           # /target, *.so, *.dylib, *.dll, *.egg-info, __pycache__, dist/
```

## Rust conventions

- Use `daft_ext::prelude::*` for all imports.
- Import `arrow_array::Array` for `len()`/`is_null()` and `arrow_array::cast::AsArray` for downcasting.
- Daft uses `LargeUtf8` (i64 offsets) for strings — downcast with `as_string::<i64>()`, never `i32`.
- Apply `#[daft_extension]` to a struct implementing `DaftExtension`.
- Register each function in `install()` via `session.define_function(Arc::new(MyFn))`.
- Each function is a struct implementing `DaftScalarFunction` with:
  - `name(&self) -> &CStr` — use `c"<extension_name>_<fn_name>"` prefix to avoid collisions.
  - `return_field(&self, args: &[Field]) -> DaftResult<Field>` — validate arg count and types,
    return `Err(DaftError::TypeError(...))` for violations.
  - `call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef>` — compute over Arrow arrays,
    propagate nulls, return `Err(DaftError::RuntimeError(...))` for failures.

## Python conventions

- Each function wrapper calls `daft.get_function("<extension_name>_<fn_name>", *args)`.
- Use `TYPE_CHECKING` guard for `Expression` import.
- Add type hints and a docstring to each wrapper.
````
