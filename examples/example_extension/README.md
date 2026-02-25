# Daft Extension SDK

Daft extensions are native scalar functions written in Rust, distributed as Python
packages, and loaded into a session at runtime. The design follows the Postgres
extension model:

| Postgres                              | Daft                                                       |
|---------------------------------------|------------------------------------------------------------|
| SQL file (CREATE FUNCTION signatures) | Python module (`__init__.py` with typed function wrappers) |
| C shared object (implementation)      | Rust cdylib (implementation via `daft-ext` crate)          |
| `CREATE EXTENSION`                    | `session.load_extension(my_extension)`                     |

Extension authors ship a normal pip-installable Python package. Users import
functions directly and get full IDE support — autocomplete, type hints, docstrings.
The native implementations are linked at runtime by loading the extension into a
session.

## Quick Start

```python
import daft

# Step 1. Import the python function definition
from my_extension import increment

# Step 2. Load the extension into the active session
daft.load_extension(daft_ext_example)

# Step 3. You can now use 'increment'
df = daft.from_pydict({"x": [1, 2, 3]})
df.select(increment(daft.col("x"))).collect()
# {"result": [2, 3, 4]}
```

```sh
cd examples/example_extension

# Build the extension cdylib
cargo build -p daft-ext-example

# Run the tests
pytest -v
```

## Extension Structure

An extension is a Python package containing both Python signatures and a Rust
cdylib:

```
my_extension/
    __init__.py           # Python API: function signatures, types, docs
    _native/
        __init__.py       # Loads the shared library
        lib_native.dylib  # Compiled cdylib
    Cargo.toml            # Rust project producing a cdylib
    src/
        lib.rs            # Native implementations
```

### Python Side — The Signatures

```python
# my_extension/__init__.py
from __future__ import annotations
from typing import TYPE_CHECKING
from daft.session import get_function

if TYPE_CHECKING:
    from daft.expressions import Expression

def increment(expr: Expression) -> Expression:
    """Adds 1 to each value in an int32 column."""
    return get_function("increment", expr)
```

These are real Python functions with proper signatures, type hints, and docstrings.
They are importable immediately — no install-time code generation. Each function
calls `get_function(name, ...)` which resolves the native implementation by name
from the current session's function registry.

If `load_extension` has not been called, resolution fails with a clear error:
`"function 'increment' not found in session"`.

### Rust Side — The Implementations

```toml
# my_extension/Cargo.toml
[package]
name = "my-extension"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
daft-ext = { path = "../../src/daft-ext" }
arrow-array = "57"
arrow-schema = "57"
```

```rust
// my_extension/src/lib.rs
use arrow_array::Int32Array;
use arrow_schema::DataType;
use daft_ext::prelude::*;

#[daft_extension]
struct MyExtension;

impl DaftExtension for MyExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(IncrementFn));
    }
}

struct IncrementFn;

impl DaftScalarFunction for IncrementFn {
    fn name(&self) -> &CStr {
        c"increment"
    }

    fn return_field(&self, _args: &[Field]) -> DaftResult<Field> {
        Ok(Field::new("result", DataType::Int32, false))
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        let input = args[0].as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| DaftError::TypeError("expected Int32".into()))?;
        let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(Arc::new(output))
    }
}
```

Extension authors implement `DaftExtension::install`, which receives a
`&mut dyn DaftSession` and registers functions via `define_function`. Each function
implements the `DaftScalarFunction` trait:

- `name() -> &CStr` — must match the name used in the Python wrapper's
  `get_function` call
- `return_field(&[Field]) -> DaftResult<Field>` — type-checking at plan time
- `call(&[ArrayRef]) -> DaftResult<ArrayRef>` — execution at runtime

The `#[daft_extension]` proc macro generates the `daft_module_magic` entry point
symbol that Daft's loader resolves via `dlopen`.

## Architecture

### Crate Layout

```
src/daft-ext-abi/       Stable #[repr(C)] types defining the ABI contract.
src/daft-ext-core/      Safe Rust SDK (DaftScalarFunction, traits, error types).
src/daft-ext-macros/    Proc macro (#[daft_extension]).
src/daft-ext/           Facade crate — the single dependency for extension authors.
src/daft-ext-internal/  Host-side adapters (ScalarFunctionHandle, module loader). Not published.
```

The first four crates have zero dependency on Daft internals. They depend only on
each other and on `arrow` (for the Arrow C Data Interface). `daft-ext-internal`
links FFI types into Daft's internal `ScalarUDF` / `ScalarFunctionFactory` traits.

### The `daft-ext` Facade

Extension authors depend on a single crate:

```rust
// daft-ext/src/lib.rs
pub use daft_ext_abi as abi;
pub use daft_ext_core::*;
pub use daft_ext_macros::*;

pub mod prelude {
    pub use std::{ffi::CStr, sync::Arc};
    pub use daft_ext_core::prelude::*;
    pub use daft_ext_macros::daft_extension;
}
```

The prelude provides everything needed: `DaftScalarFunction`, `DaftExtension`,
`DaftSession`, `SessionContext`, `DaftError`, `DaftResult`, `Field`, `ArrayRef`,
`Arc`, `CStr`, and the `#[daft_extension]` macro.

### How the FFI Vtable Bridges ScalarUDF

`ScalarUDF` is Daft's internal trait for scalar functions. It cannot cross a
`dlopen` boundary because it uses Rust trait objects with unstable ABI layouts.

`FFI_ScalarFunction` in `daft-ext-abi` is the stable C ABI projection:

```c
// repr(C)
struct FFI_ScalarFunction {
    ctx:              *const c_void,                           // opaque, owned by module
    name:             fn(ctx) -> *const c_char,                // null-terminated UTF-8
    get_return_field: fn(ctx, args, n, ret, errmsg) -> c_int,  // Arrow C Data Interface schemas
    call:             fn(ctx, args, schemas, n, ret, ret_schema, errmsg) -> c_int,  // Arrow arrays
    fini:             fn(ctx),                                 // destructor
}
```

| ScalarUDF method                 | FFI_ScalarFunction field                              | Data crossing the boundary     |
|----------------------------------|-------------------------------------------------------|--------------------------------|
| `name()`                         | `name(ctx) -> *const c_char`                          | C string                       |
| `get_return_field(args, schema)` | `get_return_field(ctx, schemas, n, ret, err)`         | Arrow C Data Interface schemas |
| `call(args, ctx)`                | `call(ctx, arrays, schemas, n, ret, ret_schema, err)` | Arrow C Data Interface arrays  |

On the host side, `ScalarFunctionHandle` in `daft-ext-internal` wraps each
`FFI_ScalarFunction` and implements both `ScalarUDF` and `ScalarFunctionFactory`:

```
Extension cdylib                       Daft internals
─────────────────                      ──────────────
DaftScalarFunction                     ScalarFunctionHandle
  ├─ call(&[ArrayRef])                   ├─ impl ScalarUDF
  └─ return_field(&[Field])              │    ├─ call(): Series → FFI → fn → FFI → Series
                                         │    └─ get_return_field(): Field → FFI_ArrowSchema → fn → FFI_ArrowSchema → Field
       ↕ (into_ffi)                     │         ↕ (wraps vtable)
  FFI_ScalarFunction (#[repr(C)])  ───── Arc<Inner { ffi, module }>
```

The `Inner` struct bundles the `FFI_ScalarFunction` with an `Arc<ModuleHandle>`
to keep the module's `free_string` function pointer alive. On drop, it calls `fini`
to release the extension-side resources.

### Session-Scoped Function Registry

Extension functions are scoped to the session that loaded them. The session stores
functions in a unified `Bindings<ScalarFunction>`:

```rust
enum ScalarFunction {
    Python(WrappedUDFClass),                    // Python UDFs
    Native(Arc<dyn ScalarFunctionFactory>),     // Extension functions
}

struct SessionState {
    // ...
    functions: Bindings<ScalarFunction>,  // unified registry
}
```

Function resolution order:

1. Current session's `Bindings<ScalarFunction>` (Python UDFs + native extensions)
2. Global `FUNCTION_REGISTRY` (built-in functions)
3. Error: `"function 'x' not found in session"`

This means:
- Two sessions do not share extension functions
- Built-in functions are always available regardless of session
- Session functions shadow built-ins if names collide

### Loading Flow

```
session.load_extension(daft_ext_example)
    │
    ▼
Python: _get_shared_lib(module) — finds *.dylib/*.so in package dir
    │
    ▼
Rust: Session::load_and_init_extension(path)
    │
    ├─► daft_ext_internal::module::load_module(path)
    │     ├─ dlopen(path) → Library
    │     ├─ lib.get("daft_module_magic") → FFI_Module
    │     ├─ validate daft_abi_version == DAFT_ABI_VERSION
    │     └─ cache in process-global MODULES (keeps Library alive)
    │
    ├─► create FFI_SessionContext with define_function callback
    │
    └─► call module.init(&mut ffi_ctx)
          │
          └─ extension calls session.define_function(Arc<dyn DaftScalarFunction>)
               │
               ├─ into_ffi(func) → FFI_ScalarFunction vtable
               ├─ callback to host: define_function_cb(ctx, ffi)
               │    ├─ into_scalar_function_factory(ffi, module) → ScalarFunctionHandle
               │    └─ session.attach_function(name, factory) → Bindings<ScalarFunction>
               └─ done
```

`load_extension` on the Python side accepts three forms:
- **Module object**: `sess.load_extension(daft_ext_example)` — finds the `.dylib`/`.so` by scanning the package directory
- **Path object**: `sess.load_extension(Path("/path/to/lib.dylib"))`
- **String path**: `sess.load_extension("/path/to/lib.dylib")`

### Expression Resolution Flow

When a user calls `increment(col("x"))`:

```
Python: increment(col("x"))
    │
    ▼
get_function("increment", col("x"))  →  session.get_function(name, *args)
    │
    ▼
Rust: session.get_function("increment") → ScalarFunction::Native(factory)
    │
    ├─ factory.get_function(args, schema) → BuiltinScalarFnVariant::Sync(Arc<ScalarFunctionHandle>)
    │
    ▼
Expr::ScalarFn(ScalarFn::Builtin(BuiltinScalarFn { func, inputs }))
    │
    ▼
(expression is resolved — planner and executor work unchanged)
```

The expression captures a resolved `Arc<dyn ScalarUDF>` eagerly. By the time it
enters the logical plan, there is no name-based lookup. The logical planner,
physical planner, and execution engine have no `Session` reference and require
no changes.

### Arrow C Data Interface

Data crosses the extension boundary using the Arrow C Data Interface
(`FFI_ArrowArray` + `FFI_ArrowSchema`). This is a zero-copy protocol when array
layouts are compatible.

**Outbound (Daft → Extension):**
```
Series → series.to_arrow() → ArrayRef → export_arrow_array() → FFI_ArrowArray + FFI_ArrowSchema
```

**Inbound (Extension → Daft):**
```
FFI_ArrowArray + FFI_ArrowSchema → import_arrow_array() → ArrayRef → Series::from_arrow(field, array)
```

The extension FFI uses `arrow::ffi` directly (not the `common-arrow-ffi` crate
which is coupled to PyO3).

### typetag Serialization

`ScalarUDF` uses `#[typetag::serde]` for plan serialization. typetag uses
compile-time registration, so dynamically loaded types cannot register themselves.

`ScalarFunctionHandle` is compiled into Daft and registered with typetag at
compile time. It serializes by storing the function name. On deserialization, it
reconstructs with `inner: None` — calling a deserialized handle without the
extension loaded returns an error. Serialized plans referencing extension functions
are only valid if the extension is loaded in the deserializing session.

### Panic Safety

FFI calls from the extension side go through a `trampoline` wrapper that catches
panics at the boundary, converting them to error return codes with messages. This
prevents undefined behavior from unwinding across the C ABI boundary.

## Lifecycle

1. **Import** — `from my_extension import increment` works immediately. These are
   plain Python functions. No session required.

2. **Load** — `session.load_extension(my_extension)` dlopen's the cdylib and
   registers native implementations in the session's function registry.

3. **Use** — `df.select(increment(col("x")))` resolves `"increment"` from the
   session's registry and builds an expression containing the resolved
   `ScalarFunctionHandle`.

4. **Execute** — The execution engine calls `ScalarFunctionHandle::call`, which
   marshals Series through Arrow FFI, invokes the extension's function pointer,
   and marshals the result back.

5. **Session end** — When the session is dropped, its `Bindings<ScalarFunction>`
   are dropped, releasing `Arc<Inner>` references. The underlying `Library`
   remains in the process-global `MODULES` cache (symbols must stay valid for the
   process lifetime).

There is no uninstall. Extension lifetime is scoped to the session.

## Error Handling

| Scenario                                       | Error                                                   |
|------------------------------------------------|---------------------------------------------------------|
| `increment(col("x"))` without `load_extension` | `"function 'increment' not found in session"`           |
| `load_extension` with wrong ABI version        | `"extension 'x' has ABI version 2, expected 1"`         |
| `load_extension` with missing symbol           | `"symbol 'daft_module_magic' not found in 'path': ..."` |
| `load_extension(module)` with no native lib    | `"No native library (*.dylib) found in 'path'"`         |
| `load_extension` with missing file             | `"failed to canonicalize 'path': No such file"`         |
| Extension `call` returns error                 | Propagated as `DaftError::InternalError`                |
| Extension `get_return_field` returns error     | Propagated as `DaftError::InternalError`                |
| Extension init returns non-zero                | `"extension init failed with code N"`                   |

## Future Work

Not in scope for v1, but the architecture supports:

- **Aggregate UDFs** — add an aggregate vtable alongside `FFI_ScalarFunction`
- **Table functions** — register scan sources via extensions
- **Extension metadata** — version, author, description in `FFI_Module`
- **Vtable versioning** — reserved fields or version flag for backward-compatible
  vtable evolution
