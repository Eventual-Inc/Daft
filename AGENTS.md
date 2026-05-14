# Resources

- https://docs.daft.ai for the user-facing API docs
- CONTRIBUTING.md for detailed development process
- https://github.com/Eventual-Inc/Daft for issues, discussions, and PRs

# Dev Workflow

1. [Once] Set up Python environment and install dependencies: `make .venv`
2. [Optional] Activate .venv: `source .venv/bin/activate`. Not necessary with Makefile commands.
3. If Rust code is modified, rebuild: `make build`
4. Run tests. See [Testing Details](#testing-details).

# Testing Details

- `make test` runs tests in `tests/` directory. Uses `pytest` under the hood.
  - Must set `DAFT_RUNNER` environment variable to `ray` or `native` to run the tests with the corresponding runner.
    - Start with `DAFT_RUNNER=native` unless testing Ray or distributed code.
  - `make test EXTRA_ARGS="..."` passes additional arguments to `pytest`.
    - `make test EXTRA_ARGS="-v tests/dataframe/test_select.py"` runs the test in the given file.
    - `make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe"` runs the given test method.
  - Default `integration`, `benchmark`, and `hypothesis` tests are disabled. Best to run on CI.
- `make doctests` runs doctests in `daft/` directory. Tests docstrings in Daft APIs.

# PR Conventions

- Titles: Conventional Commits format; enforced by `.github/workflows/pr-labeller.yml`.
- Descriptions: follow `.github/pull_request_template.md`.

## Cursor Cloud specific instructions

- **Daft is a Python+Rust library, not a web service.** There are no long-running services to start. The development loop is: `make .venv` → `make build` → run tests/scripts.
- The Rust build (`make build`) takes ~4-5 minutes on a fresh compile. Incremental rebuilds after small Rust changes are much faster.
- The `make build` target uses `maturin develop --uv`, which compiles the Rust extension and installs it into the venv in-place. A `patchelf` warning may appear — it is harmless and can be ignored.
- Always use `DAFT_RUNNER=native` for tests unless specifically testing distributed/Ray functionality. Example: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/dataframe/test_select.py"`.
- Lint commands: `ruff check daft/` for Python, `cargo clippy --all-features` for Rust. The full pre-commit suite is available via `make check-format`.
- `make doctests` runs docstring examples; useful for verifying API doc correctness.
- The `uv` package manager and Python 3.11 must be on `$PATH`. If using the update script, `$HOME/.local/bin` is added to PATH for this purpose.
