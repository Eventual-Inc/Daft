use url::{ParseError, Position};

use crate::Error;

pub(crate) const DELIMITER: &str = "/";
pub fn parse_object_url(uri: &str) -> super::Result<(String, String, String)> {
    let parsed = url::Url::parse(uri).map_err(|source| Error::InvalidUrl {
        path: uri.into(),
        source,
    })?;

    let bucket = match parsed.host_str() {
        Some(s) => Ok(s),
        None => Err(Error::InvalidUrl {
            path: uri.into(),
            source: ParseError::EmptyHost,
        }),
    }?;

    // Use raw `uri` for object key: URI special character escaping might mangle key
    let bucket_scheme_prefix_len = parsed[..Position::AfterHost].len();
    let key = uri[bucket_scheme_prefix_len..].trim_start_matches(DELIMITER);

    Ok((
        parsed.scheme().to_string(),
        bucket.to_string(),
        key.to_string(),
    ))
}
