use std::fmt::Display;
use std::fmt::Formatter;
#[derive(Clone, Default)]
pub struct S3Config {
    pub region_name: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<String>,
    pub access_key: Option<String>,
}

#[derive(Clone, Default)]
pub struct IOConfig {
    pub s3: S3Config,
}

impl Display for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "S3Config
    region_name: {:?}
    endpoint_url: {:?}
    key_id: {:?}
    access_key: {:?}",
            self.region_name, self.endpoint_url, self.key_id, self.access_key
        )
    }
}

impl Display for IOConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "IOConfig:
{}",
            self.s3
        )
    }
}
