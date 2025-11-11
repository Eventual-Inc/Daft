use std::collections::HashMap;

#[cfg(feature = "python")]
use pyo3::{
    Bound, PyResult,
    types::{PyAnyMethods, PyDict, PyDictMethods},
};

pub trait MetricsCollector: Send {
    fn inc_counter(&mut self, name: &str, value: u64);
}

#[derive(Default, Clone)]
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
        if value == 0 {
            return;
        }
        *self.counters.entry(name.to_string()).or_insert(0) += value;
    }
}

pub struct NoopMetricsCollector;

impl MetricsCollector for NoopMetricsCollector {
    fn inc_counter(&mut self, _name: &str, _value: u64) {}
}

#[cfg(feature = "python")]
pub fn operator_metrics_from_pydict(metrics: &Bound<'_, PyDict>) -> PyResult<OperatorMetrics> {
    let mut operator_metrics = OperatorMetrics::default();

    if let Some(counters_any) = metrics.get_item("counters")? {
        let counters = counters_any.cast::<PyDict>()?;
        for (name, value) in counters.iter() {
            let metric_name: String = name.extract()?;
            let amount: u64 = value.extract()?;
            operator_metrics.counters.insert(metric_name, amount);
        }
    }

    Ok(operator_metrics)
}
