use tonic::transport::Server;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_tree::HierarchicalLayer;
use daft_connect::DaftSparkConnectService;
use daft_connect::spark_connect::spark_connect_service_server::SparkConnectServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = HierarchicalLayer::new(2);

    let subscriber = Registry::default()
        .with(env_filter)
        .with(fmt_layer);
    
    tracing::subscriber::set_global_default(subscriber).unwrap();


    let addr = "[::1]:50051".parse()?;
    let service = DaftSparkConnectService::default();

    println!("Daft-Connect server listening on {}", addr);

    Server::builder()
        .add_service(SparkConnectServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}