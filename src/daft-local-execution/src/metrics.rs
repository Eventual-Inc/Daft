use common_metrics::{ATTR_NODE_ID, ATTR_NODE_TYPE, ops::NodeInfo};
use opentelemetry::KeyValue;

pub(crate) fn key_values_from_node_info(node_info: &NodeInfo) -> Vec<KeyValue> {
    vec![
        KeyValue::new(ATTR_NODE_ID, node_info.id.to_string()),
        KeyValue::new(ATTR_NODE_TYPE, node_info.node_type.to_string()),
    ]
}
