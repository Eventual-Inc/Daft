use tokio::runtime::{Builder, Runtime};
use tonic::{
    transport::{Channel, Endpoint},
    IntoRequest, Response,
};

use crate::v1::protos::tron::{tron_service_client::TronServiceClient, RunRequest, RunResponse};


/// Simple client error type
type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Simple client result type
type Result<T, E = Error> = ::std::result::Result<T, E>;

/// Rust client for the Echo service.
#[derive(Debug)]
pub struct TronClient {
    client: TronServiceClient<Channel>,
    rt: Runtime,
}

impl TronClient {
    /// Create's an echo service client.
    pub fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Error>,
    {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let client = rt.block_on(TronServiceClient::connect(dst))?;
        Ok(Self { client, rt })
    }

    /// Run the query.
    pub fn run(
        &mut self,
        request: impl IntoRequest<RunRequest>,
    ) -> Result<Response<RunResponse>, tonic::Status> {
        self.rt.block_on(self.client.run(request))
    }
}
