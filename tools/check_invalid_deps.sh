#!/bin/bash
set -e
# Error if Daft depends on openssl or native-tls
cargo tree --workspace --all-features | grep -vzqE "(openssl-sys|native-tls)"
# Error if Daft without Python feature depends on pyo3 (except common-arrow-ffi which is python-only)
cargo tree --workspace --exclude common-arrow-ffi | grep -vzqE "pyo3"
