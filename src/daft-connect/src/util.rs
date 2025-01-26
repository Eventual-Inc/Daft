use std::net::ToSocketAddrs;

use snafu::whatever;

use crate::error::{ConnectError, ConnectResult};

pub fn parse_spark_connect_address(addr: &str) -> ConnectResult<std::net::SocketAddr> {
    // Check if address starts with "sc://"
    if !addr.starts_with("sc://") {
        whatever!("Address must start with 'sc://'");
    }

    // Remove the "sc://" prefix
    let addr = addr.trim_start_matches("sc://");

    // Resolve the hostname using tokio's DNS resolver
    let addrs = addr
        .to_socket_addrs()
        .map_err(|e| ConnectError::internal(e.to_string()))?;

    // Take the first resolved address
    let Some(nxt) = addrs.into_iter().next() else {
        whatever!("No addresses found for hostname")
    };
    Ok(nxt)
}

/// An extension trait that adds the method `required` to any Option.
/// Useful for extracting the values out of protobuf fields as they are always wrapped in options
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to a different type, returning an
    /// error if None.
    fn required(self, field: impl Into<String>) -> Result<T, ConnectError>;
}

impl<T> FromOptionalField<T> for Option<T> {
    fn required(self, field: impl Into<String>) -> Result<T, ConnectError> {
        match self {
            None => Err(ConnectError::invalid_relation(format!(
                "Required field '{}' is missing",
                field.into()
            ))),
            Some(t) => Ok(t),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spark_connect_address_valid() {
        let addr = "sc://localhost:10009";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_spark_connect_address_missing_prefix() {
        let addr = "localhost:10009";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must start with 'sc://'"));
    }

    #[test]
    fn test_parse_spark_connect_address_invalid_port() {
        let addr = "sc://localhost:invalid";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_spark_connect_address_missing_port() {
        let addr = "sc://localhost";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_spark_connect_address_empty() {
        let addr = "";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must start with 'sc://'"));
    }

    #[test]
    fn test_parse_spark_connect_address_only_prefix() {
        let addr = "sc://";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert_eq!(err, "invalid socket address");
    }

    #[test]
    fn test_parse_spark_connect_address_ipv4() {
        let addr = "sc://127.0.0.1:10009";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_spark_connect_address_ipv6() {
        let addr = "sc://[::1]:10009";
        let result = parse_spark_connect_address(addr);
        assert!(result.is_ok());
    }
}
