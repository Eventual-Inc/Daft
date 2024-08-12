#!/bin/bash
cargo tree --workspace --all-features | grep -v 'rustls-pemfile' | grep -vzq 'rustls'
