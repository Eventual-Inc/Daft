use futures::StreamExt;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::DaftResult,
};

pub fn url_download<S: ToString>(urls: &[S]) -> DaftResult<Vec<u8>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let fetches = futures::stream::iter(urls.iter().enumerate().map(|(i, url)| {
        let owned_url: String = url.to_string();
        tokio::spawn(async move {
            match reqwest::get(owned_url).await {
                Ok(result) => Ok((i, result.bytes().await?)),
                Err(error) => Err(error),
            }
        })
    }))
    .buffer_unordered(32)
    .map(|f| match f {
        Ok(Ok((i, bytes))) => Some((i, bytes)),
        _ => None,
    });

    let mut results = rt.block_on(async move {
        // let mut result = vec![];

        fetches
            .filter_map(|b| async move { b })
            .collect::<Vec<_>>()
            .await

        // let _sizes = fetches.map(|b| match b {
        //     Some((i, bytes)) => {
        //         result.extend_from_slice(bytes.as_ref());
        //         bytes.len()
        //     }
        //     None => 0,
        // }).collect::<Vec<_>>().await;
        // result
    });

    results.sort_by_key(|k| k.0);
    let data = results
        .iter()
        .map(|(_, b)| b.as_ref())
        .collect::<Vec<_>>()
        .concat();

    println!("data len: {}", data.len());
    Ok(data)
}

impl Utf8Array {
    pub fn url_download(&self) -> DaftResult<BinaryArray> {
        let urls = self.as_arrow().values_iter().collect::<Vec<_>>();
        let _data = url_download(urls.as_slice())?;
        Ok(BinaryArray::empty(
            self.name(),
            &crate::datatypes::DataType::Binary,
        ))
    }
}
