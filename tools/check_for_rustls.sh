#!/bin/bash
cargo tree --workspace --all-features | grep -v 'rustls-pemfile' | grep -v 'rustls-pki-types' | grep -vzq 'rustls'
