#![expect(
    clippy::derive_partial_eq_without_eq,
    reason = "prost does not properly derive Eq"
)]

tonic::include_proto!("spark.connect");
