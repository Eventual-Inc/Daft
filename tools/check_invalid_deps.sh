#!/bin/bash
# Error if Daft depends on openssl or native-tls
cargo tree --workspace --all-features | grep -vzqE "(openssl-sys|native-tls)"
# Error if Daft without Python feature depends on pyo3
cargo tree --workspace | grep -vzqE "pyo3"
# Error if arrow2 is used outside of daft-arrow and serde_arrow
num_arrow2_deps=$(cargo tree -i arrow2 --depth 1 --prefix=none -q | wc -l)
if (( $num_arrow2_deps != 2 )); then
    echo "Error: arrow2 is used outside of daft-arrow and serde_arrow"
    exit 1
fi
