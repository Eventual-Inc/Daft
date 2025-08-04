use http_body_util::{BodyExt, Empty, Full};
use hyper::{header, Response, StatusCode};
use serde::Serialize;

use crate::Res;

/// This bypasses CORS restrictions for the dashboard.
///
/// # Note
/// We allow all origins (*) because:
/// 1. This is a local development tool running on localhost
/// 2. It needs to work in various Jupyter environments (VS Code, JupyterLab, Colab, etc.)
/// 3. It doesn't handle sensitive data requiring strict CORS protection
fn cors() -> &'static str {
    "*"
}

fn response_builder(status: StatusCode, body: Option<impl Serialize>) -> Res {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, cors())
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST")
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type")
        .body(body.map_or_else(
            || {
                Empty::default()
                    .map_err(|infallible| match infallible {})
                    .boxed()
            },
            |body| {
                Full::new(serde_json::to_string(&body).unwrap().into())
                    .map_err(|infallible| match infallible {})
                    .boxed()
            },
        ))
        .unwrap()
}

pub fn with_body(status: StatusCode, body: impl Serialize) -> Res {
    response_builder(status, Some(body))
}

pub fn empty(status: StatusCode) -> Res {
    response_builder(status, None::<String>)
}
