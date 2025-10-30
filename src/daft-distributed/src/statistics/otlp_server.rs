use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use opentelemetry_proto::tonic as otelpb;
use otelpb::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    metrics::v1::{metric::Data, number_data_point::Value as NumberValue},
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

type AggregationKey = (String, String, String); // (query_id, node_id, metric_name)
type AggregationMap = HashMap<AggregationKey, u64>;

#[derive(Clone, Default)]
pub struct OtlpMetricsServer {
    agg: Arc<Mutex<AggregationMap>>,
}

impl OtlpMetricsServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_service(self) -> MetricsServiceServer<Self> {
        MetricsServiceServer::new(self)
    }

    #[allow(dead_code)]
    pub fn aggregator(&self) -> Arc<Mutex<AggregationMap>> {
        self.agg.clone()
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsServer {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();

        for resource_metrics in req.resource_metrics {
            // Collect resource-level attributes
            let mut resource_attrs: HashMap<String, String> = HashMap::new();
            if let Some(resource) = resource_metrics.resource {
                for attr in resource.attributes {
                    if let Some(val) = attr.value.and_then(|v| v.value) {
                        let v = otlp_any_value_to_string(val);
                        resource_attrs.insert(attr.key, v);
                    }
                }
            }

            for scope_metrics in resource_metrics.scope_metrics {
                for metric in scope_metrics.metrics {
                    let metric_name = metric.name.clone();
                    match metric.data {
                        Some(Data::Sum(sum)) => {
                            for dp in sum.data_points {
                                let (value, dp_attrs) = extract_number_datapoint(dp);
                                let mut all_attrs = resource_attrs.clone();
                                all_attrs.extend(dp_attrs);

                                // Print a concise line for visibility
                                println!(
                                    "OHOH: println metric: {}, value: {}, attrs: {}",
                                    metric_name,
                                    value,
                                    format_attributes(&all_attrs)
                                );

                                // Update aggregation: prefer query_id if present, else ""; node id by key "id"
                                let query_id = all_attrs
                                    .get("daft.query_id")
                                    .or_else(|| all_attrs.get("query_id"))
                                    .cloned()
                                    .unwrap_or_default();
                                let node_id = all_attrs.get("id").cloned().unwrap_or_default();

                                let mut agg = self.agg.lock().await;
                                let entry = agg
                                    .entry((query_id, node_id, metric_name.clone()))
                                    .or_insert(0);
                                *entry = value.saturating_add(*entry);
                            }
                        }
                        Some(Data::Gauge(gauge)) => {
                            for dp in gauge.data_points {
                                let (value, dp_attrs) = extract_number_datapoint(dp);
                                let mut all_attrs = resource_attrs.clone();
                                all_attrs.extend(dp_attrs);
                                println!(
                                    "OHOH: println metric: {}, value: {}, attrs: {}",
                                    metric_name,
                                    value,
                                    format_attributes(&all_attrs)
                                );

                                let query_id = all_attrs
                                    .get("daft.query_id")
                                    .or_else(|| all_attrs.get("query_id"))
                                    .cloned()
                                    .unwrap_or_default();
                                let node_id = all_attrs.get("id").cloned().unwrap_or_default();
                                let mut agg = self.agg.lock().await;
                                let entry = agg
                                    .entry((query_id, node_id, metric_name.clone()))
                                    .or_insert(0);
                                *entry = value; // gauge treated as last value
                            }
                        }
                        _ => {
                            // Ignore other types for now
                        }
                    }
                }
            }
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

fn extract_number_datapoint(
    dp: otelpb::metrics::v1::NumberDataPoint,
) -> (u64, HashMap<String, String>) {
    let mut attrs = HashMap::new();
    for attr in dp.attributes {
        if let Some(val) = attr.value.and_then(|v| v.value) {
            let v = otlp_any_value_to_string(val);
            attrs.insert(attr.key, v);
        }
    }
    let value = match dp.value {
        Some(NumberValue::AsInt(v)) => v as u64,
        Some(NumberValue::AsDouble(v)) => v as u64,
        None => 0,
    };
    (value, attrs)
}

fn otlp_any_value_to_string(v: otelpb::common::v1::any_value::Value) -> String {
    use otelpb::common::v1::any_value::Value::*;
    match v {
        StringValue(s) => s,
        IntValue(i) => i.to_string(),
        DoubleValue(f) => f.to_string(),
        BoolValue(b) => b.to_string(),
        ArrayValue(arr) => {
            let values = arr
                .values
                .into_iter()
                .filter_map(|av| av.value)
                .map(otlp_any_value_to_string)
                .collect::<Vec<_>>();
            format!("[{}]", values.join(","))
        }
        KvlistValue(kvs) => {
            let pairs = kvs
                .values
                .into_iter()
                .filter_map(|kv| kv.value.and_then(|v| v.value).map(|val| (kv.key, val)))
                .map(|(k, v)| format!("{}={}", k, otlp_any_value_to_string(v)))
                .collect::<Vec<_>>();
            format!("{{{}}}", pairs.join(","))
        }
        BytesValue(_b) => "<bytes>".to_string(),
    }
}

fn format_attributes(attrs: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = attrs.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

pub async fn serve_otlp_metrics(listen_addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    let server = OtlpMetricsServer::new();
    let svc = server.into_service();
    tracing::info!(target = "DaftOTLPServer", address = %listen_addr, "starting OTLP metrics gRPC server");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(listen_addr)
        .await
}
