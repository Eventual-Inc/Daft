use url::{ParseError, Position};

use crate::Error;

#[derive(Debug)]
pub struct ObjectPath {
    pub scheme: String,
    pub bucket: String,
    pub key: String,
}

pub(crate) const DELIMITER: &str = "/";

/// Parse the scheme, bucket and key from the object url.
pub fn parse_object_url(uri: &str) -> super::Result<ObjectPath> {
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

    Ok(ObjectPath {
        scheme: parsed.scheme().to_string(),
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}
