#!/bin/bash
# error if Daft depends on openssl or native-tls
cargo tree --workspace --all-features | grep -vzqE "(openssl-sys|native-tls)"
