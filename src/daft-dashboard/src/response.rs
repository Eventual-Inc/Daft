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
    #[cfg(debug_assertions)]
    {
        /// During development of the dashboard, the dev server attaches to port 3000 instead.
        /// Thus, we enable CORS for that port as well.
        ///
        /// # Note
        /// We only do this for debug builds.
        const BUN_DEV_PORT: u16 = 3238;

        format!("http://127.0.0.1:{}", BUN_DEV_PORT)
    }

    #[cfg(not(debug_assertions))]
    {
        format!("http://localhost:{}", super::SERVER_PORT)
    }
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
