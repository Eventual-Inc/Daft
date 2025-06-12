use daft_proto::echo::{
    echo_server::{Echo, EchoServer},
    EchoRequest, EchoResponse,
};
use tonic::{transport::Server, Request, Response, Status};

/// EchoService implementation
struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        println!("echo service received a request.");
        let req = request.into_inner();
        let res = EchoResponse {
            message: format!("ECHO: {}", req.message),
        };
        Ok(Response::new(res))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let echo_addr = "[::1]:50051".parse().unwrap();
    let echo_service = EchoService {};

    println!("EchoServer listening on {echo_addr}");
    Server::builder()
        .add_service(EchoServer::new(echo_service))
        .serve(echo_addr)
        .await?;
    Ok(())
}
