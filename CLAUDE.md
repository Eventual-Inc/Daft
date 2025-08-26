# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Development Commands

### Build and Setup
- `make .venv` - Set up virtual environment with all dependencies
- `make build` - Compile and install Daft for development (includes all extras)
- `make build-release` - Compile optimized release build (use for benchmarks/performance testing)
- `make build-whl` - Build wheel file without installation

### Testing
- `make test` - Run Python test suite (excludes integration tests)
- `DAFT_RUNNER=native make test` - Run tests with native (local) runner
- `DAFT_RUNNER=ray make test` - Run tests with Ray (distributed) runner
- `DAFT_RUNNER=native pytest path/to/test.py::test_function` - Run specific test
- `cargo test --workspace` - Run all Rust tests
- `cargo test -p daft-core` - Run tests for specific Rust crate

### Code Quality
- `make format` - Format Python and Rust code
- `make lint` - Lint Python and Rust code
- `make check-format` - Check if code is properly formatted
- `make precommit` - Run all pre-commit hooks

### Documentation
- `make docs` - Build documentation
- `make docs-serve` - Build docs in development server

## Codebase Architecture

Daft is a distributed DataFrame query engine with a Rust core and Python bindings. The architecture follows a layered approach:

### Core Data Layer
- **`daft-core`**: Foundation with Series, Schema, DataTypes built on Arrow2
- **`daft-recordbatch`**: Primary data container with expression evaluation
- **`daft-micropartition`**: Lazy-loaded data chunks with metadata optimization

### Query Processing Pipeline
1. **`daft-dsl`**: Expression language and function definitions
2. **`daft-logical-plan`**: High-level query representation with optimization
3. **`daft-physical-plan`**: Executable plans with partitioning strategies
4. **`daft-local-execution`**: Single-machine pipeline execution
5. **`daft-distributed`**: Multi-machine actor-based execution

### I/O and Data Sources
- **`daft-scan`**: Scan planning with pushdown optimization
- **`daft-io`**: Cloud storage abstraction (S3, GCS, Azure)
- **`daft-parquet`**, **`daft-csv`**, **`daft-json`**: Format-specific readers
- **`daft-writers`**: Output to various formats including Delta Lake/Iceberg

### Specialized Functions
- **`daft-functions-utf8`**: String operations
- **`daft-functions-list`**: Array/list processing
- **`daft-functions-json`**: JSON manipulation
- **`daft-functions-temporal`**: Date/time operations

## Key Development Patterns

### Adding New Expressions
When adding functions, follow this pattern:
1. Implement `ScalarUDF` trait in appropriate `daft-functions-*` crate
2. Register function in crate's `FunctionModule::register()`
3. Add Python binding in `daft/expressions/expressions.py`
4. Add Series method in `daft/series.py`
5. Write tests using the `test_expression` fixture

### Testing Strategy
- Python tests focus on DataFrame/Expression functionality in `tests/`
- Rust tests validate kernel implementations at crate level
- Use `DAFT_RUNNER=native` for local testing, `DAFT_RUNNER=ray` for distributed
- Integration tests in `tests/integration/` require cloud credentials

### Memory Management
- MicroPartitions can be Loaded/Unloaded for memory efficiency
- Use streaming execution with pipeline backpressure
- Vectorized operations on Arrow2 arrays for performance

### Error Handling
- Use `DaftResult<T>` consistently in Rust code
- Expression evaluation handles null propagation automatically
- Schema validation occurs during planning phase

## Development Workflow

1. **Environment Setup**: Run `make .venv && make hooks`
2. **Development**: Edit code, run `make build` after Rust changes
3. **Testing**: Use `make test` or specific pytest commands
4. **Code Quality**: Run `make format && make lint` before commits
5. **Documentation**: Update docstrings and run `make docs` if needed

## Important Notes

- Always use `make build` (not `make build-release`) for development
- Set `DAFT_RUNNER` environment variable to choose execution backend
- Python docstrings must include runnable `doctest` examples
- Rust code uses workspace-level Cargo.toml for dependency management
- Pre-commit hooks enforce formatting and basic checks
