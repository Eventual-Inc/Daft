use http_body_util::{BodyExt, Empty, Full};
use hyper::{header, Response, StatusCode};
use serde::Serialize;

use crate::Res;

/// This bypasses CORS restrictions.
///
/// # Note
/// If you are running the web application from another port than [`super::SERVER_PORT`], you will need to change
/// the below port. If you do not, you will get a CORS policy error.
fn cors() -> String {
    format!("http://localhost:{}", super::SERVER_PORT)
}

fn response_builder(status: StatusCode, body: Option<impl Serialize>) -> Res {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, cors())
        .body(body.map_or_else(
            || {
                Empty::default()
                    .map_err(|infallible| match infallible {})
                    .boxed()
            },
            |body| {
                let body =
                    serde_json::to_string(&body).expect("Body should always be serializable");
                Full::new(body.into())
                    .map_err(|infallible| match infallible {})
                    .boxed()
            },
        ))
        .expect("Responses should always be able to be constructed")
}

pub fn with_body(status: StatusCode, body: impl Serialize) -> Res {
    response_builder(status, Some(body))
}

pub fn empty(status: StatusCode) -> Res {
    response_builder(status, None::<String>)
}
