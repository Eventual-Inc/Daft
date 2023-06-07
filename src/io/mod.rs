use futures::StreamExt;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    error::DaftResult,
};

pub fn url_download<S: ToString>(name: &str, urls: &[S]) -> DaftResult<BinaryArray> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let fetches = futures::stream::iter(urls.iter().enumerate().map(|(i, url)| {
        let owned_url: String = url.to_string();
        tokio::spawn(async move {
            match reqwest::get(owned_url).await {
                Ok(result) => (i, result.bytes().await),
                Err(error) => (i, Err(error)),
            }
        })
    }))
    .buffer_unordered(32)
    .map(|f| match f {
        Ok((i, Ok(bytes))) => (i, Some(bytes)),
        Ok((i, Err(err))) => (i, None),
        Err(err) => panic!("this shouldnt happen"),
    });

    let mut results = rt.block_on(async move {
        fetches
            .collect::<Vec<_>>()
            .await
    });

    results.sort_by_key(|k| k.0);
    let mut offsets: Vec<i64> = Vec::with_capacity(urls.len() + 1);
    let data = {
        let mut to_concat = Vec::with_capacity(urls.len());

        for (i, b) in results.iter() {
            match b {
                Some(b) => {
                    to_concat.push(b.as_ref());
                    offsets.push(b.len() as i64 + offsets.last().unwrap());
                }
                None => {
                    offsets.push(*offsets.last().unwrap());
                }
            }
        }
        to_concat.concat()
    };
    BinaryArray::try_from((name, data, offsets))
}

impl Utf8Array {
    pub fn url_download(&self) -> DaftResult<BinaryArray> {
        let urls = self.as_arrow().values_iter().collect::<Vec<_>>();
        url_download(self.name(), urls.as_slice())
    }
}
