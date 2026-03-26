use common_metrics::{ATTR_NODE_ID, ATTR_NODE_TYPE};
use opentelemetry::KeyValue;

use super::PipelineNodeContext;

pub(crate) fn key_values_from_context(context: &PipelineNodeContext) -> Vec<KeyValue> {
    vec![
        KeyValue::new(ATTR_NODE_ID, context.node_id.to_string()),
        KeyValue::new(ATTR_NODE_TYPE, context.node_type.to_string()),
    ]
}
