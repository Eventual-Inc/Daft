# daft-scan

Defines the `ScanOperator` trait, `ScanTask` struct, and built-in operator implementations (`GlobScanOperator`, `AnonymousScanOperator`). Also owns scan-related primitives: `Pushdowns`, `PartitionField`, `Sharder`, and predicate rewriting for partition pruning.
