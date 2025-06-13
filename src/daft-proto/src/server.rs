use daft_ir::schema::Schema;
use daft_proto::{
    v1::protos::tron::{
        tron_service_server::{TronService, TronServiceServer},
        RunRequest, RunResponse,
    },
    FromToProto,
};
use tonic::{transport::Server, Request, Response, Status};

/// Tron service implementation.
struct TronServiceImpl;

#[tonic::async_trait]
impl TronService for TronServiceImpl {
    async fn run(&self, request: Request<RunRequest>) -> Result<Response<RunResponse>, Status> {
        let req = request.into_inner();
        let schema = Schema::from_proto(req.schema.unwrap()).expect("failed to parse schema");

        println!("RECEIVED SCHEMA: ");
        println!("------------------");
        println!("{:?}", &schema);

        Ok(Response::new(RunResponse::default()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tron_address = "[::1]:50051".parse().unwrap();
    let tron_service = TronServiceImpl {};

    println!("TronService listening on {tron_address}");
    Server::builder()
        .add_service(TronServiceServer::new(tron_service))
        .serve(tron_address)
        .await?;
    Ok(())
}
