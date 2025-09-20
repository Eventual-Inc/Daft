#!/bin/bash
# Error if Daft depends on openssl or native-tls
cargo tree --workspace --all-features | grep -vzqE "(openssl-sys|native-tls)"
# Error if Daft without Python feature depends on pyo3
cargo tree --workspace | grep -vzqE "pyo3"
