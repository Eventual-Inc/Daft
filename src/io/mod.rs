use futures::StreamExt;
use reqwest;

use tokio;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::DaftResult,
};

pub fn url_download<S: ToString>(urls: &[S]) -> DaftResult<Vec<u8>> {
    let mut results = Vec::with_capacity(urls.len());

    let fetches = futures::stream::iter(urls.iter().map(|url| {
        let owned_url: String = url.to_string();
        tokio::spawn(async move {
            match reqwest::get(owned_url).await {
                Ok(result) => result.bytes().await,
                Err(error) => Err(error),
            }
        })
    }))
    .buffered(32)
    .map(|f| match f {
        Ok(Ok(bytes)) => Some(bytes),
        _ => None,
    });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let data = rt.block_on(async move {
        let mut result = vec![];
        let _ = fetches.map(|b| match b {
            Some(bytes) => {
                result.extend_from_slice(bytes.as_ref());
            }
            None => println!("None"),
        });
        result
    });
    println!("data len: {}", data.len());
    Ok(data)
}

impl Utf8Array {
    pub fn url_download(&self) -> DaftResult<BinaryArray> {
        let urls = self.as_arrow().values_iter().collect::<Vec<_>>();
        let data = url_download(urls.as_slice())?;
        Ok(BinaryArray::empty(
            self.name(),
            &crate::datatypes::DataType::Binary,
        ))
    }
}
