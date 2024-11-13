use daft_connect::DaftSparkConnectService;
use spark_connect::spark_connect_service_server::SparkConnectServiceServer;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, Registry};
use tracing_tracy::TracyLayer;

fn setup_tracing() {
    tracing::subscriber::set_global_default(
        Registry::default().with(TracyLayer::default()).with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_file(true)
                .with_line_number(true),
        ),
    )
    .expect("setup tracing subscribers");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let addr = "[::1]:50051".parse()?;
    let service = DaftSparkConnectService::default();

    info!("Daft-Connect server listening on {}", addr);

    tokio::select! {
        result = Server::builder()
            .add_service(SparkConnectServiceServer::new(service))
            .serve(addr) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("\nReceived Ctrl-C, gracefully shutting down server");
        }
    }

    Ok(())
}
