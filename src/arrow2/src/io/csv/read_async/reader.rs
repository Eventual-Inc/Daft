use futures::AsyncRead;

use super::{AsyncReader, ByteRecord};
use crate::io::csv::read;

use crate::error::{Error, Result};

/// Asynchronosly read `rows.len` rows from `reader` into `rows`, skipping the first `skip`.
/// This operation has minimal CPU work and is thus the fastest way to read through a CSV
/// without deserializing the contents to Arrow.
pub async fn read_rows<R>(
    reader: &mut AsyncReader<R>,
    skip: usize,
    rows: &mut [ByteRecord],
) -> Result<usize>
where
    R: AsyncRead + Unpin + Send,
{
    // skip first `start` rows.
    let mut row = ByteRecord::new();
    for _ in 0..skip {
        let res = reader.read_byte_record(&mut row).await;
        if !res.unwrap_or(false) {
            break;
        }
    }

    let mut row_number = 0;
    for row in rows.iter_mut() {
        let has_more = reader
            .read_byte_record(row)
            .await
            .map_err(|e| Error::External(format!(" at line {}", skip + row_number), Box::new(e)))?;
        if !has_more {
            break;
        }
        row_number += 1;
    }
    Ok(row_number)
}

/// Synchronously read `rows.len` rows from `reader` into `rows`. This is used in the local i/o case.
pub fn local_read_rows<R>(
    reader: &mut read::Reader<R>,
    rows: &mut [read::ByteRecord],
    limit: Option<usize>,
) -> Result<(usize, bool)>
where
    R: std::io::Read,
{
    let mut row_number = 0;
    let mut has_more = true;
    for row in rows.iter_mut() {
        if matches!(limit, Some(limit) if row_number >= limit) {
            break;
        }
        has_more = reader
            .read_byte_record(row)
            .map_err(|e| Error::External(format!(" at line {}", row_number), Box::new(e)))?;
        if !has_more {
            break;
        }
        row_number += 1;
    }
    Ok((row_number, has_more))
}
