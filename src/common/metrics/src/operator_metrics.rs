use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub trait MetricsCollector: Send {
    fn inc_counter(&mut self, name: &str, value: u64);
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    counters: HashMap<String, u64>,
}

impl IntoIterator for OperatorMetrics {
    type Item = (String, u64);
    type IntoIter = std::collections::hash_map::IntoIter<String, u64>;
    fn into_iter(self) -> Self::IntoIter {
        self.counters.into_iter()
    }
}

impl MetricsCollector for OperatorMetrics {
    fn inc_counter(&mut self, name: &str, value: u64) {
        match self.counters.get_mut(name) {
            Some(count) => *count += value,
            None => {
                self.counters.insert(name.to_string(), value);
            }
        }
    }
}

pub struct NoopMetricsCollector;

impl MetricsCollector for NoopMetricsCollector {
    fn inc_counter(&mut self, _name: &str, _value: u64) {}
}
