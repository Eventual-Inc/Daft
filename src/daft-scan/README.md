# daft-scan

This crate is used to create scan operators in logical plans.

## Contributing

Suppose your format is `jsonl` - you should add two things.

1. Export top-level `scan_jsonl` which takes all arguments.
2. Export `builder::JsonlScanBuilder`
