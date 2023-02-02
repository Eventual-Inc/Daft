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

1. Ensure that your system has a suitable Python version installed (>=3.7)
2. [Install the Rust compilation toolchain](https://www.rust-lang.org/tools/install)
3. Clone the Daft repo: `git clone git@github.com:Eventual-Inc/Daft.git`
4. Run `make venv` from your new cloned Daft repository to create a new virtual environment with all of Daft's development dependencies installed
5. Run `make hooks` to install pre-commit hooks: these will run tooling on every commit to ensure that your code meets Daft development standards

### Developing

1. `make build`: recompile your code after modifying any Rust code in `src/`
2. `make test`: run tests
3. `DAFT_RUNNER=ray make test`: set the runner to the Ray runner and run tests (DAFT_RUNNER defaults to `py`)
