use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub trait MetricsCollector: Send {
    fn inc_counter(
        &mut self,
        name: &str,
        value: u64,
        description: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorCounter {
    pub value: u64,
    pub description: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl OperatorCounter {
    fn new(value: u64, description: Option<&str>, attributes: HashMap<String, String>) -> Self {
        Self {
            value,
            description: description.map(|d| d.to_string()),
            attributes,
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    counters: HashMap<String, OperatorCounter>,
}

impl OperatorMetrics {
    pub fn inc_counter(
        &mut self,
        name: &str,
        value: u64,
        description: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) {
        match self.counters.get_mut(name) {
            Some(entry) => {
                entry.value += value;

                // If the description is not set, set it if it is provided
                if entry.description.is_none()
                    && let Some(desc) = description
                {
                    entry.description = Some(desc.to_string());
                }

                // Extend the attributes if they are provided
                if let Some(attrs) = attributes {
                    entry.attributes.extend(attrs);
                }
            }
            None => {
                self.counters.insert(
                    name.to_string(),
                    OperatorCounter::new(value, description, attributes.unwrap_or_default()),
                );
            }
        }
    }
}

impl IntoIterator for OperatorMetrics {
    type Item = (String, OperatorCounter);
    type IntoIter = std::collections::hash_map::IntoIter<String, OperatorCounter>;
    fn into_iter(self) -> Self::IntoIter {
        self.counters.into_iter()
    }
}

impl MetricsCollector for OperatorMetrics {
    fn inc_counter(
        &mut self,
        name: &str,
        value: u64,
        description: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) {
        self.inc_counter(name, value, description, attributes);
    }
}

pub struct NoopMetricsCollector;

impl MetricsCollector for NoopMetricsCollector {
    fn inc_counter(
        &mut self,
        _name: &str,
        _value: u64,
        _description: Option<&str>,
        _attributes: Option<HashMap<String, String>>,
    ) {
    }
}
