use async_stream::try_stream;
use futures::pin_mut;
use prost::Message;
use tonic::{metadata::MetadataMap, transport::Server, Request, Response, Status, Streaming};

use arrow_format::flight::data::*;
use arrow_format::flight::service::flight_service_server::{FlightService, FlightServiceServer};

use super::{Result, TonicStream};

use crate::{AUTH_PASSWORD, AUTH_USERNAME};

pub async fn scenario_setup(port: u16) -> Result {
    let addr = super::listen_on(port).await?;
    let svc = FlightServiceServer::new(Service {});

    let server = Server::builder().add_service(svc).serve(addr);

    // NOTE: Log output used in tests to signal server is ready
    println!("Server listening on localhost:{}", addr.port());
    server.await?;
    Ok(())
}

#[derive(Clone)]
struct Service {}

impl Service {
    fn check_auth(&self, metadata: &MetadataMap) -> Result<String, Status> {
        metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .and_then(|username| (username == AUTH_USERNAME).then(|| AUTH_USERNAME.to_string()))
            .ok_or_else(|| Status::unauthenticated("Invalid token"))
    }
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = TonicStream<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = TonicStream<Result<FlightInfo, Status>>;
    type DoGetStream = TonicStream<Result<FlightData, Status>>;
    type DoPutStream = TonicStream<Result<PutResult, Status>>;
    type DoActionStream = TonicStream<Result<arrow_format::flight::data::Result, Status>>;
    type ListActionsStream = TonicStream<Result<ActionType, Status>>;
    type DoExchangeStream = TonicStream<Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("do_get"))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let stream = request.into_inner();

        let stream = try_stream! {
            pin_mut!(stream);
            for await item in stream {
                let HandshakeRequest {payload, ..} = item.map_err(|_| Status::invalid_argument(format!("Invalid"))).unwrap();
                yield handle(&payload, AUTH_USERNAME, AUTH_PASSWORD).unwrap()
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let metadata = request.metadata();
        self.check_auth(metadata)?;
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let username = self.check_auth(request.metadata())?;
        // Respond with the authenticated username.
        let result = arrow_format::flight::data::Result {
            body: username.as_bytes().to_vec(),
        };
        let output = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("do_exchange"))
    }
}

fn handle(payload: &[u8], username: &str, password: &str) -> Result<HandshakeResponse> {
    let auth = BasicAuth::decode(payload)?;

    if auth.username == username && auth.password == password {
        Ok(HandshakeResponse {
            payload: username.as_bytes().to_vec(),
            ..HandshakeResponse::default()
        })
    } else {
        Err(Box::new(Status::unauthenticated(format!(
            "Don't know user {}",
            auth.username
        ))))
    }
}
