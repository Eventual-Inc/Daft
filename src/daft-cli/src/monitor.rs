use std::net::SocketAddr;

use dashmap::DashMap;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

struct SimpleMetricsService {
    operator_metrics: DashMap<u64, DashMap<String, Value>>
}

impl SimpleMetricsService {
    pub fn new() -> Self {
        Self { operator_metrics: DashMap::new() }
    }

    pub fn into_service(self) -> MetricsServiceServer<Self> {
        MetricsServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MetricsService for SimpleMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();

        // Clear screen and move cursor to top-left
        print!("\x1b[2J\x1b[H");

        for resource_metric in req.resource_metrics {
            for scope_metrics in resource_metric.scope_metrics {
                for metric in scope_metrics.metrics {
                    let name = metric.name;
                    let pieces = name.split('.').collect::<Vec<&str>>();
                    let operator_id = pieces[1].parse::<u64>().unwrap();
                    let metric_name = pieces[2];
                    let metrics = self.operator_metrics.entry(operator_id).or_insert(DashMap::new());

                    let value = if let Some(data) = metric.data {
                        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
                        match data {
                            Data::Gauge(g) => {
                                g.data_points.last().map(|p| p.value).flatten()
                            }
                            Data::Sum(s) => {
                                s.data_points.last().map(|p| p.value).flatten()
                            }
                            other => unimplemented!("Unsupported metric data type: {:?}", other),
                        }
                    } else {
                        None
                    };

                    if let Some(value) = value {
                        metrics.insert(metric_name.to_string(), value);
                    }
                }
            }

            for op_entry in self.operator_metrics.iter() {
               let op_id = op_entry.key();
               let metrics = op_entry.value();
               let metrics_str = metrics.iter().map(|entry| {
                    let k = entry.key();
                    let v = entry.value();
                    let v_str = match v {
                        Value::AsInt(v) => v.to_string(),
                        Value::AsDouble(v) => v.to_string(),
                    };
                    format!("{v_str} {k}")
                }).collect::<Vec<String>>().join(", ");
                println!("Operator {}: {}", op_id, metrics_str);
            }
        }

        Ok(Response::new(ExportMetricsServiceResponse { partial_success: None }))
    }
}

pub async fn serve_otlp_metrics(addr: SocketAddr, shutdown_rx: oneshot::Receiver<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting OTLP metrics gRPC collector on {}", addr);
    let server = SimpleMetricsService::new();
    let svc = server.into_service();
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, async move {
            let _ = shutdown_rx.await;
        })
        .await?;
    Ok(())
}
