set -ex

pushd ${1}/rust/integration-testing
CARGO_INCREMENTAL=false cargo build --target-dir ../target
popd
