# Contributing to Daft

Daft is an open-source project and we welcome contributions from the community. Whether you're reporting bugs, proposing features, or contributing code, this guide will help you get started.

## Quick Start

- **Found a bug?** 🐛 [Report it here](#reporting-issues)
- **Have a feature idea?** 💡 [Start a discussion](#proposing-features)
- **Want to make your first PR?** 🚀 [Contribute new code](#contributing-code)

## Reporting Issues

To report bugs and issues with Daft, please file an issue on our [issues](https://github.com/Eventual-Inc/Daft/issues) page.

Additionally, please include the following information in your bug report:

1. Operating system
2. Daft version
3. Python version
4. Daft runner (native or Ray)

## Proposing Features

We highly encourage you to propose new features or ideas. Please start a GitHub Discussion in our [Ideas channel](https://github.com/Eventual-Inc/Daft/discussions/categories/ideas).

When proposing features, please include:

1. Feature Summary (no more than 3 sentences)
2. Example usage (pseudo-code to show how it is used)
3. Corner-case behavior (how should this code behave in various corner-case scenarios)

## Contributing Code

> **💡 Already set up?**
>
>  See our quick [tutorial](#adding-new-expressions) on how to add a new expression to Daft.

### Development Environment

To set up your development environment:

1. Ensure that your system has a suitable Python version installed (>=3.9, <=3.12)
2. [Install the Rust compilation toolchain](https://www.rust-lang.org/tools/install)
3. Install [bun](https://bun.sh/) in order to build docs and the daft-dashboard functionality.
4. Install [cmake](https://cmake.org/). If you use [homebrew](https://brew.sh), you can run `brew install cmake`.
5. Install [protoc](https://protobuf.dev/installation/). You will need this for release builds -- `make build-release`. With homebrew, installation is `brew install protobuf`.
6. Clone the Daft repo: `git clone git@github.com:Eventual-Inc/Daft.git`
7. Run `make .venv` from your new cloned Daft repository to create a new virtual environment with all of Daft's development dependencies installed
8. Run `make hooks` to install pre-commit hooks: these will run tooling on every commit to ensure that your code meets Daft development standards

### Developing

1. `make build`: recompile your code after modifying any Rust code in `src/`
2. `DAFT_RUNNER=native make test`: run tests
3. `DAFT_RUNNER=ray make test`: set the runner to the Ray runner and run tests
4. `make docs`: build docs
5. `make docs-serve`: build docs in development server
6. `make format`: format all Python and Rust code
7. `make lint`: lint all Python and Rust code
8. `make check-format`: check that all Python and Rust code is formatted, alias `make format-check`
9. `make precommit`: run all pre-commit hooks, must install pre-commit first(pip install pre-commit)
10. `make build-release`: perform a full release build of Daft
11. `make build-whl`: recompile your code after modifying any Rust code in `src/` for development, only generate `whl` file without installation
12. `make daft-proto`: build Daft proto sources in `src/daft-proto`

#### Note about Developing `daft-dashboard`

If you wish to enable, or work on the daft-dashboard functionality, it does have an additional dependency of [bun.sh](https://bun.sh/).

You simply need to install bun, and everything else should work out of the box!

### Developing with Ray

Running a development version of Daft on a local Ray cluster is as simple as including `daft.context.set_runner_ray()` in your Python script and then building and executing it as usual.

To use a remote Ray cluster, run the following steps on the same operating system version as your Ray nodes, in order to ensure that your binaries are executable on Ray.

1. `mkdir wd`: this is the working directory, it will hold all the files to be submitted to Ray for a job
2. `ln -s daft wd/daft`: create a symbolic link from the Python module to the working directory
3. `make build-release`: an optimized build to ensure that the module is small enough to be successfully uploaded to Ray. Run this after modifying any Rust code in `src/`
4. `ray job submit --working-dir wd --address "http://<head_node_host>:8265" -- python script.py`: submit `wd/script.py` to be run on Ray

### Debugging

The debugging feature uses a special VSCode launch configuration to start the Python debugger with a script at `tools/attach_debugger.py`, which takes the target script's name as input. This script finds the process ID, updates the launch.json file, compiles the target script, and runs it. It then attaches a Rust debugger to the Python debugger, allowing both to work together. Breakpoints in Python code hit the Python debugger, while breakpoints in Rust code hit the Rust debugger.

#### Preparation

- **CodeLLDB Extension for Visual Studio Code**:
  This extension is useful for debugging Rust code invoked from Python.

- **Setting Up the Virtual Environment Interpreter**
  (Ctrl+Shift+P -> Python: Select Interpreter -> .venv)

- **Debug Settings in launch.json**
  This file is usually found in the `.vscode` folder of your project root. See the [official VSCode documentation](https://code.visualstudio.com/docs/editor/debugging#_launch-configurations) for more information about the launch.json file.
  <details><summary><code><b>launch.json</b></code></summary>

      ```json
      {
          "configurations": [
              {
                  "name": "Debug Rust/Python",
                  "type": "debugpy",
                  "request": "launch",
                  "program": "${workspaceFolder}/tools/attach_debugger.py",
                  "args": [
                      "${file}"
                  ],
                  "console": "internalConsole",
                  "serverReadyAction": {
                      "pattern": "pID = ([0-9]+)",
                      "action": "startDebugging",
                      "name": "Rust LLDB"
                  }
              },
              {
                  "name": "Rust LLDB",
                  "pid": "0",
                  "type": "lldb",
                  "request": "attach",
                  "program": "${command:python.interpreterPath}",
                  "stopOnEntry": false,
                  "sourceLanguages": [
                      "rust"
                  ],
                  "presentation": {
                      "hidden": true
                  }
              }
          ]
      }
      ```

      </details>

#### Running the debugger

1. Create a Python script containing Daft code. Ensure that your virtual environment is set up correctly.

2. Set breakpoints in any `.rs` or `.py` file.

3. In the `Run and Debug` panel on the left, select `Debug Rust/Python` from the drop-down menu on top and click the `Start Debugging` button. This will start a debugging session using the file that is currently opened in the VSCode editor.

At this point, your debugger should stop on breakpoints in any .rs file located within the codebase.

> **Note**:
> On some systems, the LLDB debugger will not attach unless [ptrace protection](https://linux-audit.com/protect-ptrace-processes-kernel-yama-ptrace_scope) is disabled.
> To disable, run the following command:
>
> ```shell
> echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope
> ```

### Testing

We run test suites across Python and Rust. Python tests focus on high-level DataFrame and Expression functionality, while Rust tests validate individual kernel implementations at a lower level.

#### Python tests

Our python tests are located in the `tests` directory, you can run all the tests at once with `make tests`.

To run specific tests, set the runner for the tests in the environment and then run the tests directly using [pytest]("https://doc.rust-lang.org/cargo/commands/cargo-test.html").

```
DAFT_RUNNER=native pytest tests/dataframe
```

#### Rust tests

Our rust tests are distributed across crates, you can run all tests with `cargo test --no-default-features --workspace`.

To run rust tests that call into Python, the `--features python` flag and libpython3.*.so dynamic libraries are required. Please ensure that these are installed, here's a table of common locations on different os:

| Operating System        | Package Manager | Architecture      | Library Path Pattern                          |
|-------------------------|----------------|-------------------|-----------------------------------------------|
| **Ubuntu/Debian**       | apt            | x86_64            | `/usr/lib/x86_64-linux-gnu/libpython3.x.so.1.0` |
|                         |                | Other             | `/usr/lib/libpython3.x.so.1.0`                |
| **Red Hat/CentOS**      | yum/dnf        | x86_64            | `/usr/lib64/libpython3.x.so.1.0`               |
| **macOS (Homebrew)**    | Homebrew       | Intel             | `/usr/local/opt/python@3.x/lib/libpython3.x.dylib` |
|                         |                | Apple Silicon     | `/opt/homebrew/opt/python@3.x/lib/libpython3.x.dylib` |
| **macOS (System)**      | Installer      | All               | `/Library/Frameworks/Python.framework/Versions/3.x/lib/libpython3.x.dylib` |

Set environment variables to locate the Python library:

```sh
export PYO3_PYTHON=".venv/bin/python"
export PYO3_PYTHON_PYLIB="/usr/lib/x86_64-linux-gnu/libpython3.11.so.1"
export RUSTFLAGS="-C link-arg=-Wl,-rpath,${PYO3_PYTHON_PYLIB%/*} -C link-arg=-L${PYO3_PYTHON_PYLIB%/*} -C link-arg=-lpython3.11"
```

Execute the test after configuration:

```sh
cargo test -p daft-dsl --features python -- expr::tests
```

### Benchmarking

Benchmark tests are located in `tests/benchmarks`. If you would like to run benchmarks, make sure to first do `make build-release` instead of `make build` in order to compile an optimized build of Daft.

1. `pytest tests/benchmarks/[test_file.py] -m benchmark`: Run all benchmarks in a file
2. `pytest tests/benchmarks/[test_file.py] -k [test_name] -m benchmark`: Run a specific benchmark in a file

More information about writing and using benchmarks can be found on the [pytest-benchmark docs](https://pytest-benchmark.readthedocs.io/en/latest/).

### Adding new expressions

Since new expressions are a very common feature request, we wanted to make it easy for new contributors to add these. Adding a new expression requires implementation in Rust and exposing it to Python.

#### Step 1: Implement the function in Rust

Add your function to the appropriate crate (`daft-functions-json`, `daft-functions-utf8`, etc.).
For more advanced use cases, see existing implementations in [daft-functions-utf8](src/daft-functions-utf8/src/lib.rs)

```rs
// This prelude defines all required ScalarUDF dependencies.
use daft_dsl::functions::prelude::*;

// We need these for the trait.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct MyToUpperCase;

#[typetag::serde]
impl ScalarUDF for MyToUpperCase {

    // Start by giving the function a name.
    // This will be the name used in SQL when calling the function.
    fn name(&self) -> &'static str {
        "to_uppercase"
    }

    // Then we add an implementation for it.
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let s = inputs.required(0)?;
        // Note: using into_iter is not the most performant way of implementing this, but for this example, we don't care about performance.
        let arr = s
            .utf8()
            .expect("type should have been validated already during `get_return_field`")
            .into_iter()
            .map(|s_opt| s_opt.map(|s| s.to_uppercase()))
            .collect::<Utf8Array>();
        Ok(arr.into_series())
    }

    // We also need a `get_return_field` which is used during planning to ensure that the args and datatypes are compatible.
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        /// grab the first positional value from `inputs`
        let input = inputs.required(0)?.to_field(schema)?;
        // make sure the input is a string datatype
        ensure!(input.dtype.is_string(), "expected string");
        Ok(input)
    }

    // Finally, we want a brief docstring for the function. This is used when generating the sql documentation.
    fn docstring(&self) -> &'static str {
        "Converts a string to uppercase."
    }
}
```

#### Step 2: Register the function

Okay, now that we have the actual function implementation available, we're not quite done yet. We also need to register this to our `FUNCTION_REGISTRY` which is a global registry of all expressions/functions.

Whatever crate/module you are in, there should be a `daft_dsl::functions::FunctionModule` implementation that registers all of the functions. So all you need to do is add your new struct into there.

for the `utf8` functions, it's defined here `src/daft-functions-utf8/src/lib.rs`

```rs
impl daft_dsl::functions::FunctionModule for Utf8Functions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        // ...
        parent.add_fn(MyToUpperCase); // add this line here
    }
}
```

#### Step 3: Add python bindings

Create expression method in `daft/expressions/expressions.py`

```py
# the method name should usually match that of which you defined in your `ScalarUDF` implementation
def to_uppercase(self) -> Expression:
    # make sure to add a docstring with a runnable `doctest` example
    """Convert UTF-8 string to all upper.
    Returns:
        Expression: a String expression which is `self` uppercased

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
        >>> df = df.select(df["x"].to_uppercase())
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ FOO  │
        ├╌╌╌╌╌╌┤
        │ BAR  │
        ├╌╌╌╌╌╌┤
        │ BAZ  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    # Get the function frou our global `FUNCTION_REGISTRY`
    f = native.get_function_from_registry("to_uppercase")
    return Expression._from_pyexpr(f(self._expr))

```

For functions with additional arguments, you will need to convert those all to expressions before calling the function_registry function.

```py
def extract_all(self, pattern: str | Expression, index: int = 0) -> Expression:
    pattern_expr = Expression._to_expression(pattern)
    idx = Expression._to_expression(index)
    f = native.get_function_from_registry("extract_all")
    # Pass scalar values as kwargs
    return Expression._from_pyexpr(f(self._expr, pattern_expr._expr, index=idx._expr))
```

Add Series method in `daft/series.py`:

For series, It just delegates out to the expression implementation, so we can just call the helper method `_eval_expressions`

```py
  def to_uppercase(self) -> Series:
    return self._eval_expressions("upper")
```

and for functions with additional arguments:

```py
def extract_all(self, pattern: Series, index: int = 0) -> Series:
  # Pass scalar values as kwargs
  return self._eval_expressions("extract_all", pattern, index=index)
```

#### Step 4: Write tests

For testing, you can add a new file, or update an existing one in `tests/expressions/`

We have a fixture `test_expression` that will do most of the heavy lifting and ensure that the apis are consistent across expr, series, and sql.

here's an example of testing the `extract` function using the `test_expression` fixture

```py
def test_extract(test_expression):
    test_data = ["123-456", "789-012", "345-678"]
    regex = r"(\d)(\d*)"
    expected = ["123", "789", "345"]
    test_expression(
        data=test_data,
        expected=expected,
        name="extract",
        namespace="str",
        sql_name="regexp_extract", # if this is not provided, it will be the same as `name`
        args=[regex],
        kwargs={}
    )
```
