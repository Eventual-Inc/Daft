use tonic::transport::Server;
use daft_connect::MySparkConnectService;
use daft_connect::spark_connect::spark_connect_service_server::SparkConnectServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = MySparkConnectService::default();

    println!("Daft-Connect server listening on {}", addr);

    Server::builder()
        .add_service(SparkConnectServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}