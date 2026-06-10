// TODO(native-kafka-write): remove once Task 8/9 wires accounting into the writer integration.
#[allow(dead_code)]
pub mod accounting;
// TODO(native-kafka-write): remove once Task 8/9 wires config into the writer integration.
#[allow(dead_code)]
pub mod config;
pub(crate) mod headers;
pub mod metrics;
pub(crate) mod producer;
pub(crate) mod record;
