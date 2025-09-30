//! Utilities for aiding in development
//!
//! These utilities should not be used in production
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
};

use azure_core::{
    Url,
    error::{Error, ErrorKind},
};
use oauth2::{AuthorizationCode, CsrfToken};
use tracing::debug;

use crate::authorization_code_flow::AuthorizationCodeFlow;

/// A very naive implementation of a redirect server.
///
/// A ripoff of <https://github.com/ramosbugs/oauth2-rs/blob/master/examples/msgraph.rs>, stripped
/// down for simplicity. This server blocks until redirected to.
///
/// This implementation should only be used for testing.
pub fn naive_redirect_server(
    auth_obj: &AuthorizationCodeFlow,
    port: u16,
) -> azure_core::Result<AuthorizationCode> {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    // The server will terminate itself after collecting the first code.
    if let Some(mut stream) = listener.incoming().flatten().next() {
        let mut reader = BufReader::new(&stream);

        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();

        let Some(redirect_url) = request_line.split_whitespace().nth(1) else {
            return Err(Error::with_message(ErrorKind::Credential, || {
                format!("unexpected redirect url: {request_line}")
            }));
        };
        let url = Url::parse(&("http://localhost".to_string() + redirect_url)).unwrap();

        debug!("url == {}", url);

        let code = match url.query_pairs().find(|(key, _)| key == "code") {
            Some((_, value)) => AuthorizationCode::new(value.into_owned()),
            None => {
                return Err(Error::message(
                    ErrorKind::Credential,
                    "query pair not found: code",
                ));
            }
        };

        let state = match url.query_pairs().find(|(key, _)| key == "state") {
            Some((_, value)) => CsrfToken::new(value.into_owned()),
            None => {
                return Err(Error::message(
                    ErrorKind::Credential,
                    "query pair not found: state",
                ));
            }
        };

        if state.secret() != auth_obj.csrf_state.secret() {
            return Err(Error::with_message(ErrorKind::Credential, || {
                format!(
                    "State secret mismatch: expected {}, received: {}",
                    auth_obj.csrf_state.secret(),
                    state.secret()
                )
            }));
        }

        let message = "Authentication complete. You can close this window now.";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
            message.len(),
            message
        );
        stream.write_all(response.as_bytes()).unwrap();

        return Ok(code);
    }

    unreachable!()
}
