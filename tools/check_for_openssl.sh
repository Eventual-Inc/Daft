#!/bin/bash
# error if Daft depends on openssl or native-tls
# exclude vendored azure_identity since we're vendoring it
cargo tree --workspace --all-features --exclude azure_identity | grep -vzqE "(openssl-sys|native-tls)"
