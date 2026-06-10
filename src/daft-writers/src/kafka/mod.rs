// TODO(native-kafka-write): remove once Task 9 records attempts, deliveries, and failures.
#[allow(dead_code)]
pub mod accounting;
// TODO(native-kafka-write): remove once Task 9/10 surfaces redacted Kafka config diagnostics.
#[allow(dead_code)]
pub mod config;
pub(crate) mod headers;
pub mod metrics;
pub(crate) mod producer;
pub(crate) mod record;
pub mod writer;
