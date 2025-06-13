use daft_ir::schema::Schema;
use daft_proto::{v1::protos::echo::{
    echo_server::{Echo, EchoServer},
    EchoRequest, EchoResponse,
}, FromToProto};
use tonic::{transport::Server, Request, Response, Status};

struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let req = request.into_inner();
        let schema = Schema::from_proto(req.schema.unwrap()).expect("failed to parse schema");

        println!("RECEIVED SCHEMA: ");
        println!("------------------");
        println!("{:?}", &schema);

        let res = EchoResponse {
            message: "schema was logged.".to_string(),
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
