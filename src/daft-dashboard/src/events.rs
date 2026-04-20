/// Event protocol under construction - expect breaking changes to
/// both the log file format and Rust types
use std::collections::HashMap;

use common_metrics::{BYTES_READ_KEY, BYTES_WRITTEN_KEY, DURATION_KEY, QueryEndState};
use serde::{Deserialize, de::Deserializer};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct EventLogStarted {
    pub query_id: String,
    pub timestamp: f64,
    pub daft_version: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryStarted {
    pub query_id: String,
    pub timestamp: f64,
    pub plan: String,
    pub runner: Option<String>,
    pub entrypoint: Option<String>,
    pub dashboard_url: Option<String>,
    pub daft_version: Option<String>,
    pub runner_version: Option<String>,
    pub python_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryEnded {
    pub query_id: String,
    pub timestamp: f64,
    pub state: QueryEndState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OptimizationStarted {
    pub query_id: String,
    pub timestamp: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OptimizationEnded {
    pub query_id: String,
    pub timestamp: f64,
    pub plan: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionStarted {
    pub query_id: String,
    pub timestamp: f64,
    pub physical_plan: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionEnded {
    pub query_id: String,
    pub timestamp: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct OperatorStarted {
    pub query_id: String,
    pub timestamp: f64,
    pub node_id: usize,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct OperatorEnded {
    pub query_id: String,
    pub timestamp: f64,
    pub node_id: usize,
    pub name: String,
    pub duration_ms: Option<f64>,
}

/// Public metric representation, decoupled from internal common_metrics::Stat.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MetricValue {
    Count(u64),
    Bytes(u64),
    Percent(f64),
    Float(f64),
    DurationMicros(u64),
}

fn deserialize_metrics<'de, D>(deserializer: D) -> Result<HashMap<String, MetricValue>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = HashMap::<String, Value>::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(name, value)| {
            let metric = match value {
                Value::Object(_) => serde_json::from_value::<MetricValue>(value)
                    .map_err(serde::de::Error::custom)?,
                Value::Number(n) if name == DURATION_KEY => {
                    MetricValue::DurationMicros(n.as_u64().ok_or_else(|| {
                        serde::de::Error::custom(format!(
                            "expected u64 duration metric for key {name}"
                        ))
                    })?)
                }
                Value::Number(n) if name == BYTES_READ_KEY || name == BYTES_WRITTEN_KEY => {
                    MetricValue::Bytes(n.as_u64().ok_or_else(|| {
                        serde::de::Error::custom(format!(
                            "expected u64 bytes metric for key {name}"
                        ))
                    })?)
                }
                Value::Number(n) if n.is_u64() => MetricValue::Count(n.as_u64().unwrap()),
                Value::Number(n) => MetricValue::Float(n.as_f64().ok_or_else(|| {
                    serde::de::Error::custom(format!("expected numeric metric for key {name}"))
                })?),
                _ => {
                    return Err(serde::de::Error::custom(format!(
                        "invalid metric value for key {name}"
                    )));
                }
            };
            Ok((name, metric))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Stats {
    pub query_id: String,
    pub timestamp: f64,
    pub node_id: usize,
    #[serde(deserialize_with = "deserialize_metrics")]
    pub metrics: HashMap<String, MetricValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ProcessStats {
    pub query_id: String,
    pub timestamp: f64,
    #[serde(deserialize_with = "deserialize_metrics")]
    pub metrics: HashMap<String, MetricValue>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResultProduced {
    pub query_id: String,
    pub timestamp: f64,
    #[serde(alias = "rows")]
    pub num_rows: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    EventLogStarted(EventLogStarted),
    QueryStarted(QueryStarted),
    QueryEnded(QueryEnded),
    OptimizationStarted(OptimizationStarted),
    OptimizationEnded(OptimizationEnded),
    ExecutionStarted(ExecutionStarted),
    ExecutionEnded(ExecutionEnded),
    OperatorStarted(OperatorStarted),
    OperatorEnded(OperatorEnded),
    Stats(Stats),
    ProcessStats(ProcessStats),
    ResultProduced(ResultProduced),
}
