# AGENTS.md - Building and Testing Daft

## Project Overview

Daft is a distributed query engine for large-scale multimodal data processing using Python or SQL, implemented in Rust with Python bindings. The project uses:
- **Rust** for the core engine implementation
- **Python** for the user-facing API
- **Maturin** for building Python extensions from Rust code
- **uv** for Python dependency management
- **pytest** for testing

## Prerequisites

Before building or testing, ensure you have the following installed:

1. **Rust toolchain**: Install via [rustup](https://rustup.rs/)
2. **Python 3.9+**: The project supports Python 3.9 and above
3. **uv**: Python package manager (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
4. **Additional tools** (for full functionality):
   - `bun` for dashboard functionality
   - `cmake` for certain dependencies
   - `protoc` for protocol buffer compilation

## Building the Project

### Setting Up the Environment

The project uses `uv` for dependency management, which is handled through the Makefile:

```bash
# Create virtual environment and install dependencies
make .venv

# Activate the virtual environment
# Not necessary if you use the Makefile commands
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### Building Rust Extensions

The core of Daft is written in Rust and compiled to Python extensions using Maturin:

```bash
# Development build (faster compilation, slower runtime)
make build

# Release build (slower compilation, faster runtime)
make build-release
```

### Building Protocol Buffers

If any .proto files are modified, you will need to rebuild the protocol buffers:

```bash
# Build proto sources (requires protoc to be installed)
make daft-proto
```

## Testing the Project

### Test Configuration

The project uses pytest with specific configuration in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
addopts = "-m 'not (integration or benchmark or hypothesis)'"
minversion = "6.0"
testpaths = ["tests"]
```

### Running Tests

Tests can be run using the Makefile commands:

```bash
# Basic test run
make test

# Run with specific runner (required environment variable)
DAFT_RUNNER=native make test
DAFT_RUNNER=ray make test

# Run specific test files or methods
make test EXTRA_ARGS="-v tests/dataframe/test_select.py"
make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe"

# Run with additional options
make test EXTRA_ARGS="-v --tb=short"
```

## Resources

- [Library Documentation](https://docs.daft.ai)
- [Contributing Guide](CONTRIBUTING.md)
- [GitHub Repo](https://github.com/Eventual-Inc/Daft)
