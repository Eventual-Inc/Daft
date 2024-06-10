# Contributing to Daft

## Reporting Issues

To report bugs and issues with Daft, please report in detail:

1. Operating system
2. Daft version
3. Python version
4. Runner that your code is using

## Proposing Features

Please start a GitHub Discussion in our [Ideas channel](https://github.com/Eventual-Inc/Daft/discussions/categories/ideas). Once the feature is clarified, fleshed out and approved, the corresponding issue(s) will be created from the GitHub discussion.

When proposing features, please include:

1. Feature Summary (no more than 3 sentences)
2. Example usage (pseudo-code to show how it is used)
3. Corner-case behavior (how should this code behave in various corner-case scenarios)

## Contributing Code

### Development Environment

To set up your development environment:

1. Ensure that your system has a suitable Python version installed (>=3.7, <=3.11)
2. [Install the Rust compilation toolchain](https://www.rust-lang.org/tools/install)
3. Clone the Daft repo: `git clone git@github.com:Eventual-Inc/Daft.git`
4. Run `make .venv` from your new cloned Daft repository to create a new virtual environment with all of Daft's development dependencies installed
5. Run `make hooks` to install pre-commit hooks: these will run tooling on every commit to ensure that your code meets Daft development standards

### Developing

1. `make build`: recompile your code after modifying any Rust code in `src/`
2. `make test`: run tests
3. `DAFT_RUNNER=ray make test`: set the runner to the Ray runner and run tests (DAFT_RUNNER defaults to `py`)

### Developing with Ray

Running a development version of Daft on a local Ray cluster is as simple as including `daft.context.set_runner_ray()` in your Python script and then building and executing it as usual.

To use a remote Ray cluster, run the following steps on the same operating system version as your Ray nodes, in order to ensure that your binaries are executable on Ray.

1. `mkdir wd`: this is the working directory, it will hold all the files to be submitted to Ray for a job
2. `ln -s daft wd/daft`: create a symbolic link from the Python module to the working directory
3. `make build-release`: an optimized build to ensure that the module is small enough to be successfully uploaded to Ray. Run this after modifying any Rust code in `src/`
4. `ray job submit --working-dir wd --address "http://<head_node_host>:8265" -- python script.py`: submit `wd/script.py` to be run on Ray

### Benchmarking

Benchmark tests are located in `tests/benchmarks`. If you would like to run benchmarks, make sure to first do `make build-release` instead of `make build` in order to compile an optimized build of Daft.

1. `pytest tests/benchmarks/[test_file.py] --benchmark-only`: Run all benchmarks in a file
2. `pytest tests/benchmarks/[test_file.py] -k [test_name] --benchmark-only`: Run a specific benchmark in a file

More information about writing and using benchmarks can be found on the [pytest-benchmark docs](https://pytest-benchmark.readthedocs.io/en/latest/).
