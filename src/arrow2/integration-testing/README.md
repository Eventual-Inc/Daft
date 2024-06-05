# Integration tests

This directory contains integration tests against official Arrow implementations.

They are run as part of the CI pipeline, called by apache/arrow/dev/crossbow.

The IPC files tested on the official pipeline are already tested on our own tests.

## Flight tests

To run the flight scenarios across this implementation, use

```bash
SCENARIO="auth:basic_proto"
cargo run --bin flight-test-integration-server -- --port 3333 --scenario $SCENARIO &
# wait for server to be up

cargo run --bin flight-test-integration-client -- --host localhost --port 3333 --scenario $SCENARIO
```

to run an integration test against a file, use

```bash
FILE="../testing/arrow-testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_dictionary.json.gz"
gzip -dc $FILE > generated.json

cargo build --bin flight-test-integration-server
cargo run --bin flight-test-integration-server -- --port 3333 &
cargo run --bin flight-test-integration-client -- --host localhost --port 3333 --path generated.json
# kill with `fg` and stop process
```
