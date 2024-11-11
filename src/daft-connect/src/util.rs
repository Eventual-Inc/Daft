use std::net::ToSocketAddrs;

#[macro_export]
macro_rules! invalid_argument {
    ($arg: tt) => {{
        let msg = format!($arg);
        ::tonic::Status::invalid_argument(msg)
    }};
}

#[macro_export]
macro_rules! unimplemented_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        ::tonic::Status::unimplemented(msg)
    }};
}

pub fn parse_spark_connect_address(addr: &str) -> eyre::Result<std::net::SocketAddr> {
    // Check if address starts with "sc://"
    if !addr.starts_with("sc://") {
        return Err(eyre::eyre!("Address must start with 'sc://'"));
    }

    // Remove the "sc://" prefix
    let addr = addr.trim_start_matches("sc://");

    // Resolve the hostname using tokio's DNS resolver
    let addrs = addr.to_socket_addrs()?;

    // Take the first resolved address
    addrs
        .into_iter()
        .next()
        .ok_or_else(|| eyre::eyre!("No addresses found for hostname"))
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid address format"));
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
