use std::net::SocketAddr;
use std::pin::Pin;

use arrow_format::flight::data::{FlightEndpoint, Location, Ticket};
use futures::Stream;
use tokio::net::TcpListener;

pub mod auth_basic_proto;
pub mod integration_test;
pub mod middleware;

type TonicStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

pub async fn listen_on(port: u16) -> Result<SocketAddr> {
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;

    Ok(addr)
}

pub fn endpoint(ticket: &str, location_uri: impl Into<String>) -> FlightEndpoint {
    FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket.as_bytes().to_vec(),
        }),
        location: vec![Location {
            uri: location_uri.into(),
        }],
    }
}
