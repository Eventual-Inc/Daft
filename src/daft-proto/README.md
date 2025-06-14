# daft-proto

This crate holds the daft-ir proto definitions.

## Structure

- daft-proto (definitions only)
- daft-ir (depends on daft-proto)

## TODOs

- the from_proto helpers are weird.
- test via .optimize followed by roundtrip then finish the .collect
- consider using macros or something better.
- having things return box/arc is odd and less flexible.
