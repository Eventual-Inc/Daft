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
    counters: HashMap<String, Vec<OperatorCounter>>,
}

impl OperatorMetrics {
    pub fn inc_counter(
        &mut self,
        name: &str,
        value: u64,
        description: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) {
        let attrs = attributes.unwrap_or_default();
        match self.counters.get_mut(name) {
            Some(counters) => {
                if let Some(counter) = counters
                    .iter_mut()
                    .find(|counter| counter.attributes == attrs)
                {
                    counter.value += value;

                    if counter.description.is_none()
                        && let Some(desc) = description
                    {
                        counter.description = Some(desc.to_string());
                    }
                } else {
                    counters.push(OperatorCounter::new(value, description, attrs));
                }
            }
            None => {
                self.counters.insert(
                    name.to_string(),
                    vec![OperatorCounter::new(value, description, attrs)],
                );
            }
        }
    }

    pub fn snapshot(&self) -> HashMap<String, Vec<OperatorCounter>> {
        self.counters.clone()
    }
}

impl IntoIterator for OperatorMetrics {
    type Item = (String, OperatorCounter);
    type IntoIter = OperatorMetricsIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        OperatorMetricsIntoIter {
            outer: self.counters.into_iter(),
            current: None,
        }
    }
}

pub struct OperatorMetricsIntoIter {
    outer: std::collections::hash_map::IntoIter<String, Vec<OperatorCounter>>,
    current: Option<(String, std::vec::IntoIter<OperatorCounter>)>,
}

impl Iterator for OperatorMetricsIntoIter {
    type Item = (String, OperatorCounter);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((name, inner_iter)) = &mut self.current
                && let Some(counter) = inner_iter.next()
            {
                return Some((name.clone(), counter));
            }

            match self.outer.next() {
                Some((name, counters)) => {
                    if counters.len() == 1 {
                        return Some((name, counters.into_iter().next().unwrap()));
                    } else {
                        self.current = Some((name, counters.into_iter()));
                    }
                }
                None => return None,
            }
        }
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
