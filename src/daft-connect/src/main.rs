use daft_connect::DaftSparkConnectService;
use spark_connect::spark_connect_service_server::SparkConnectServiceServer;
use tonic::transport::Server;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(tracing_tracy::TracyLayer::default()),
    )
    .expect("setup tracy layer");

    let addr = "[::1]:50051".parse()?;
    let service = DaftSparkConnectService::default();

    println!("Daft-Connect server listening on {}", addr);

    tokio::select! {
        result = Server::builder()
            .add_service(SparkConnectServiceServer::new(service))
            .serve(addr) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nReceived Ctrl-C, gracefully shutting down server");
        }
    }

    Ok(())
}
