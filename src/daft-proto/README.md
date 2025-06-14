# daft-proto

This crate holds the daft-ir proto definitions.

## Usage

We do not (currently) compile protos at build time to avoid the protoc dependency.

```shell
# rename `.build.rs` to `build.rs` to enable it, then do:
cargo build -p daft-proto
```

## TODOs

- the from_proto helpers are weird.
- test via .optimize followed by roundtrip then finish the .collect
- consider using macros or something better.
- having things return box/arc is odd and less flexible.
