#[derive(Debug, Clone, Default)]
pub struct KafkaProducerMetrics {
    pub txmsgs: i64,
    pub txmsg_bytes: i64,
    pub outbuf_msg_cnt: i64,
    pub msg_cnt: i64,
    pub msg_max: i64,
}
