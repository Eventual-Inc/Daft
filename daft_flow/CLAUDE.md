# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Daft Flow is a Rust-based library with Python bindings that provides distributed data processing capabilities. The project uses a multi-crate workspace architecture with PyO3 for Python integration.

## Build Commands

- **Build for development**: `make build` or `maturin develop`
- **Build Python package**: `maturin build`
- **Run tests**: `cargo test`
- **Run tests for specific crate**: `cargo test -p <crate-name>`

## Architecture

### Python API

The Python package defined by `pyproject.toml` is named "daft_flow" and built using Maturin. This is the main user-facing API of the project.

### Rust core

The codebase is organized as a Rust workspace with the following key crates:

- **daft-flow-pylib**: Python bindings layer using PyO3, exposes Rust functionality to Python
- **daft-flow-plan**: Planning and optimization logic
- **daft-flow-builder**: Flow construction utilities
- **daft-flow-executor**: Execution engine for processing flows
- **daft-flow-datatypes**: Core data types and structures

The main entry point is `src/lib.rs` which conditionally compiles Python bindings when the "python" feature is enabled.

### Workspace dependencies

This project is part of the larger Daft project. Daft is a dataframe library designed with subcrates that provide extremely useful functionality for `daft_flow`.

We implement a middleware layer

## Development Rules

- YOU MUST only use PyO3 rust/python binding functionality from within the `src/daft-flow-pylib` subcrate because this subcrate is correctly gated by the `python` feature flag
- YOU MUST identify the appropriate subcrate for implementing the functionality that you desire, in order to avoid leaky abstractions and keep compile-times low. In the event that you determine that there is no appropriate subcrate, you must propose the creation of a new one.
- YOU SHOULD use `thiserror` for error handling where appropriate instead of panicking. Properly introduce hierarchical errors to ensure that the user has sufficient context on where errors are happening, and make use of `thiserror` best practices.

## Development Notes

- This is currently a skeleton implementation - most crates contain placeholder "Hello, world!" functions
- Build system uses Maturin for Python package compilation
- No workspace-level Cargo.toml detected - each crate manages its own dependencies
