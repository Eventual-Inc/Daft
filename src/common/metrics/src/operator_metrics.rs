use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub trait MetricsCollector: Send {
    fn inc_counter(&mut self, name: &str, value: u64);
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    counters: HashMap<String, u64>,
}

impl OperatorMetrics {
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    pub fn clear(&mut self) {
        self.counters.clear();
    }

    pub fn merge(&mut self, other: Self) {
        for (name, value) in other.counters {
            *self.counters.entry(name).or_insert(0) += value;
        }
    }

    pub fn merge_into_collector(&self, collector: &mut dyn MetricsCollector) {
        for (name, value) in &self.counters {
            collector.inc_counter(name, *value);
        }
    }

    pub fn into_inner(self) -> HashMap<String, u64> {
        self.counters
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
