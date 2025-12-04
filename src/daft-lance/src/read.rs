use common_error::{DaftError, DaftResult};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use lance::Dataset;

pub async fn stream_lance_fragments(
    uri: &str,
    fragment_ids: &[usize],
    columns: Option<Vec<String>>,
    filter: Option<String>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    if fragment_ids.is_empty() {
        return Err(DaftError::ValueError(
            "Lance fragment id must not be empty".to_string(),
        ));
    }

    let ds = Dataset::open(uri)
        .await
        .map_err(|e| DaftError::External(Box::new(e)))?;

    let fragments =
        fragment_ids
            .iter()
            .try_fold(vec![], |mut fs, fid| match ds.get_fragment(*fid) {
                Some(f) => {
                    fs.push(f.metadata().clone());
                    Ok(fs)
                }
                None => Err(DaftError::ValueError(format!(
                    "No lance fragment found with id {}",
                    fid
                ))),
            })?;

    let mut scanner = ds.scan();
    scanner.with_fragments(fragments);

    if let Some(columns) = columns
        && let Err(e) = scanner.project(columns.as_slice())
    {
        return Err(DaftError::External(Box::new(e)));
    }

    if let Some(filter) = filter
        && let Err(e) = scanner.filter(&filter)
    {
        return Err(DaftError::External(Box::new(e)));
    }

    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DaftError::External(Box::new(e)))?;
    Ok(Box::pin(stream.map(|r| match r {
        Ok(batch) => Ok(batch.try_into()?),
        Err(e) => Err(DaftError::External(Box::new(e))),
    })))
}
