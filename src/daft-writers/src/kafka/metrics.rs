#[derive(Debug, Clone, Default)]
// TODO: wire producer metrics into the native Kafka producer implementation.
#[allow(dead_code)]
pub struct KafkaProducerMetrics {
    pub txmsgs: i64,
    pub txmsg_bytes: i64,
    pub outbuf_msg_cnt: i64,
    pub msg_cnt: i64,
    pub msg_max: i64,
}
