use tokio::runtime::{Builder, Runtime};
use tonic::{
    transport::{Channel, Endpoint},
    IntoRequest, Response,
};

use crate::protos::echo::{echo_client::EchoClient, EchoRequest, EchoResponse};

/// Simple client error type
type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Simple client result type
type Result<T, E = Error> = ::std::result::Result<T, E>;

/// Rust client for the Echo service.
pub struct EchoServiceClient {
    client: EchoClient<Channel>,
    rt: Runtime,
}

impl EchoServiceClient {
    /// Create's an echo service client.
    pub fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Error>,
    {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let client = rt.block_on(EchoClient::connect(dst))?;
        Ok(Self { client, rt })
    }

    pub fn get_echo(
        &mut self,
        request: impl IntoRequest<EchoRequest>,
    ) -> Result<Response<EchoResponse>, tonic::Status> {
        self.rt.block_on(self.client.echo(request))
    }
}
