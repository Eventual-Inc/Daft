# azure_identity

> Microsoft is developing the official Azure SDK for Rust crates and has no plans to update this unofficial crate.
> In the future we may release an official version that may have a different package name.
> If releasing an official version of this crate is important to you [let us know](https://github.com/Azure/azure-sdk-for-rust/issues/new/choose).
>
> Source for this crate can now be found in <https://github.com/Azure/azure-sdk-for-rust/tree/legacy>.
> To monitor for an official, supported version of this crate, see <https://aka.ms/azsdk/releases>.

Azure Identity crate for the unofficial Microsoft Azure SDK for Rust. This crate is part of a collection of crates: for more information please refer to [https://github.com/azure/azure-sdk-for-rust](https://github.com/azure/azure-sdk-for-rust).

This crate provides several implementations of the [azure_core::auth::TokenCredential](https://docs.rs/azure_core/latest/azure_core/auth/trait.TokenCredential.html) trait.
It is recommended to start with `azure_identity::create_credential()?`, which will create an instance of `DefaultAzureCredential` by default. If you want to use a specific credential type, the `AZURE_CREDENTIAL_KIND` environment variable may be set to a value from `azure_credential_kinds`, such as `azurecli` or `virtualmachine`.

```rust no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
   let subscription_id =
       std::env::var("AZURE_SUBSCRIPTION_ID").expect("AZURE_SUBSCRIPTION_ID required");

   let credential = azure_identity::create_credential()?;

   // Let's enumerate the Azure storage accounts in the subscription using the REST API directly.
   // This is just an example. It is easier to use the Azure SDK for Rust crates.
   let url = url::Url::parse(&format!("https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Storage/storageAccounts?api-version=2019-06-01"))?;

   let access_token = credential
       .get_token(&["https://management.azure.com/.default"])
       .await?;

   let response = reqwest::Client::new()
       .get(url)
       .header(
           "Authorization",
           format!("Bearer {}", access_token.token.secret()),
       )
       .send()
       .await?
       .text()
       .await?;

   println!("{response}");
   Ok(())
}
```

The supported authentication flows are:
* [Authorization code flow](https://docs.microsoft.com/azure/active-directory/develop/v2-oauth2-auth-code-flow).
* [Client credentials flow](https://docs.microsoft.com/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow).
* [Device code flow](https://docs.microsoft.com/azure/active-directory/develop/v2-oauth2-device-code).

License: MIT
