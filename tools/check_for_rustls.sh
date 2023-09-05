#!/bin/bash
cargo tree --workspace --all-features | grep -vzq rustls
