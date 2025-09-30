use std::str;

use async_process::Command;
use azure_core::{
    auth::{AccessToken, Secret, TokenCredential},
    error::{Error, ErrorKind, ResultExt},
    from_json,
};
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::trace;

use crate::token_credentials::cache::TokenCache;

#[cfg(feature = "old_azure_cli")]
mod az_cli_date_format {
    use azure_core::error::{ErrorKind, ResultExt};
    use serde::{Deserialize, Deserializer};
    #[cfg(not(unix))]
    use time::UtcOffset;
    use time::{
        OffsetDateTime, PrimitiveDateTime, format_description::FormatItem,
        macros::format_description,
    };

    const FORMAT: &[FormatItem] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]");

    pub fn parse(s: &str) -> azure_core::Result<OffsetDateTime> {
        // expiresOn from azure cli uses the local timezone and needs to be converted to UTC
        let dt = PrimitiveDateTime::parse(s, FORMAT)
            .with_context(ErrorKind::DataConversion, || {
                format!("unable to parse expiresOn '{s}")
            })?;
        Ok(assume_local(&dt))
    }

    #[cfg(unix)]
    /// attempt to convert `PrimitiveDateTime` to `OffsetDate` using
    /// `tz::TimeZone`.  If any part of the conversion fails, such as if no
    /// timezone can be found, then use use the value as UTC.
    pub(crate) fn assume_local(date: &PrimitiveDateTime) -> OffsetDateTime {
        let as_utc = date.assume_utc();

        // try parsing the timezone from `TZ` environment variable.  If that
        // fails, or the environment variable doesn't exist, try using
        // `TimeZone::local`.  If that fails, then just return the UTC date.
        let Some(tz) = std::env::var("TZ")
            .ok()
            .and_then(|x| tz::TimeZone::from_posix_tz(&x).ok())
            .or_else(|| tz::TimeZone::local().ok())
        else {
            return as_utc;
        };

        let as_unix = as_utc.unix_timestamp();

        // if we can't find the local time type, just return the UTC date
        let Ok(local_time_type) = tz.find_local_time_type(as_unix) else {
            return as_utc;
        };

        // if we can't convert the unix timestamp to a DateTime, just return the UTC date
        let date = as_utc.date();
        let time = as_utc.time();
        let Ok(date) = tz::DateTime::new(
            date.year(),
            u8::from(date.month()),
            date.day(),
            time.hour(),
            time.minute(),
            time.second(),
            time.nanosecond(),
            *local_time_type,
        ) else {
            return as_utc;
        };

        // if we can't then convert to unix time (with the timezone) and then
        // back into an OffsetDateTime, then return the UTC date
        let Ok(date) = OffsetDateTime::from_unix_timestamp(date.unix_time()) else {
            return as_utc;
        };

        date
    }

    /// Assumes the local offset. Default to UTC if unable to get local offset.
    #[cfg(not(unix))]
    pub(crate) fn assume_local(date: &PrimitiveDateTime) -> OffsetDateTime {
        date.assume_offset(UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse(&s).map_err(serde::de::Error::custom)
    }
}

/// The response from `az account get-access-token --output json`.
#[derive(Debug, Clone, Deserialize)]
struct CliTokenResponse {
    #[serde(rename = "accessToken")]
    pub access_token: Secret,
    #[cfg(feature = "old_azure_cli")]
    #[serde(rename = "expiresOn", with = "az_cli_date_format")]
    /// The token's expiry time formatted in the local timezone.
    /// Unfortunately, this requires additional timezone dependencies.
    /// See https://github.com/Azure/azure-cli/issues/19700 for details.
    pub local_expires_on: OffsetDateTime,
    #[serde(rename = "expires_on")]
    /// The token's expiry time in seconds since the epoch, a unix timestamp.
    /// Available in Azure CLI 2.54.0 or newer.
    pub expires_on: Option<i64>,
    pub subscription: String,
    pub tenant: String,
    #[allow(unused)]
    #[serde(rename = "tokenType")]
    pub token_type: String,
}

impl CliTokenResponse {
    pub fn expires_on(&self) -> azure_core::Result<OffsetDateTime> {
        match self.expires_on {
            Some(timestamp) => Ok(OffsetDateTime::from_unix_timestamp(timestamp)
                .with_context(ErrorKind::DataConversion, || {
                    format!("unable to parse expires_on '{timestamp}'")
                })?),
            None => {
                #[cfg(feature = "old_azure_cli")]
                {
                    Ok(self.local_expires_on)
                }
                #[cfg(not(feature = "old_azure_cli"))]
                {
                    Err(Error::message(
                        ErrorKind::DataConversion,
                        "expires_on field not found. Please use Azure CLI 2.54.0 or newer.",
                    ))
                }
            }
        }
    }
}

/// Enables authentication to Azure Active Directory using Azure CLI to obtain an access token.
#[derive(Debug)]
pub struct AzureCliCredential {
    cache: TokenCache,
}

impl Default for AzureCliCredential {
    fn default() -> Self {
        Self::new()
    }
}

impl AzureCliCredential {
    pub fn create() -> azure_core::Result<Self> {
        // TODO check `az version` to see if it's installed
        Ok(AzureCliCredential::new())
    }

    /// Create a new `AzureCliCredential`
    pub fn new() -> Self {
        Self {
            cache: TokenCache::new(),
        }
    }

    /// Get an access token for an optional resource
    async fn get_access_token(scopes: Option<&[&str]>) -> azure_core::Result<CliTokenResponse> {
        // on window az is a cmd and it should be called like this
        // see https://doc.rust-lang.org/nightly/std/process/struct.Command.html
        let program = if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "az"
        };
        let mut args = Vec::new();
        if cfg!(target_os = "windows") {
            args.push("/C");
            args.push("az");
        }
        args.push("account");
        args.push("get-access-token");
        args.push("--output");
        args.push("json");

        let scopes = scopes.map(|x| x.join(" "));

        if let Some(scopes) = &scopes {
            args.push("--scope");
            args.push(scopes);
        }

        trace!(
            "fetching credential via Azure CLI: {program} {}",
            args.join(" "),
        );

        match Command::new(program).args(args).output().await {
            Ok(az_output) if az_output.status.success() => {
                let output = str::from_utf8(&az_output.stdout)?;

                let access_token = from_json(output)?;
                Ok(access_token)
            }
            Ok(az_output) => {
                let output = String::from_utf8_lossy(&az_output.stderr);
                Err(Error::with_message(ErrorKind::Credential, || {
                    format!("'az account get-access-token' command failed: {output}")
                }))
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    Err(Error::message(ErrorKind::Other, "Azure CLI not installed"))
                }
                error_kind => Err(Error::with_message(ErrorKind::Other, || {
                    format!("Unknown error of kind: {error_kind:?}")
                })),
            },
        }
    }

    /// Returns the current subscription ID from the Azure CLI.
    pub async fn get_subscription() -> azure_core::Result<String> {
        let tr = Self::get_access_token(None).await?;
        Ok(tr.subscription)
    }

    /// Returns the current tenant ID from the Azure CLI.
    pub async fn get_tenant() -> azure_core::Result<String> {
        let tr = Self::get_access_token(None).await?;
        Ok(tr.tenant)
    }

    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        let tr = Self::get_access_token(Some(scopes)).await?;
        let expires_on = tr.expires_on()?;
        Ok(AccessToken::new(tr.access_token, expires_on))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for AzureCliCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        self.cache.get_token(scopes, self.get_token(scopes)).await
    }

    /// Clear the credential's cache.
    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.cache.clear().await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "old_azure_cli")]
    use serial_test::serial;
    #[cfg(feature = "old_azure_cli")]
    use time::macros::datetime;

    use super::*;

    #[cfg(feature = "old_azure_cli")]
    #[test]
    #[serial]
    fn can_parse_expires_on() -> azure_core::Result<()> {
        let expires_on = "2022-07-30 12:12:53.919110";
        assert_eq!(
            az_cli_date_format::parse(expires_on)?,
            az_cli_date_format::assume_local(&datetime!(2022-07-30 12:12:53.919110))
        );

        Ok(())
    }

    #[cfg(all(feature = "old_azure_cli", unix))]
    #[test]
    #[serial]
    /// test the timezone conversion works as expected on unix platforms
    ///
    /// To validate the timezone conversion works as expected, this test
    /// temporarily sets the timezone to PST, performs the check, then resets
    /// the TZ environment variable.
    fn check_timezone() -> azure_core::Result<()> {
        let before = std::env::var("TZ").ok();
        std::env::set_var("TZ", "US/Pacific");
        let expires_on = "2022-11-30 12:12:53.919110";
        let result = az_cli_date_format::parse(expires_on);

        if let Some(before) = before {
            std::env::set_var("TZ", before);
        } else {
            std::env::remove_var("TZ");
        }

        let expected = datetime!(2022-11-30 20:12:53.0).assume_utc();
        assert_eq!(expected, result?);

        Ok(())
    }

    /// Test `from_json` for `CliTokenResponse` for old Azure CLI
    #[test]
    fn read_old_cli_token_response() -> azure_core::Result<()> {
        let json = br#"
        {
            "accessToken": "MuchLonger_NotTheRealOne_Sv8Orn0Wq0OaXuQEg",
            "expiresOn": "2024-01-01 19:23:16.000000",
            "subscription": "33b83be5-faf7-42ea-a712-320a5f9dd111",
            "tenant": "065e9f5e-870d-4ed1-af2b-1b58092353f3",
            "tokenType": "Bearer"
          }
        "#;
        let token_response: CliTokenResponse = from_json(json)?;
        assert_eq!(
            token_response.tenant,
            "065e9f5e-870d-4ed1-af2b-1b58092353f3"
        );
        Ok(())
    }

    /// Test `from_json` for `CliTokenResponse` for current Azure CLI
    #[test]
    fn read_cli_token_response() -> azure_core::Result<()> {
        let json = br#"
        {
            "accessToken": "MuchLonger_NotTheRealOne_Sv8Orn0Wq0OaXuQEg",
            "expiresOn": "2024-01-01 19:23:16.000000",
            "expires_on": 1704158596,
            "subscription": "33b83be5-faf7-42ea-a712-320a5f9dd111",
            "tenant": "065e9f5e-870d-4ed1-af2b-1b58092353f3",
            "tokenType": "Bearer"
        }
        "#;
        let token_response: CliTokenResponse = from_json(json)?;
        assert_eq!(
            token_response.tenant,
            "065e9f5e-870d-4ed1-af2b-1b58092353f3"
        );
        assert_eq!(token_response.expires_on, Some(1704158596));
        assert_eq!(token_response.expires_on()?.unix_timestamp(), 1704158596);
        Ok(())
    }
}
