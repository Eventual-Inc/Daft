use std::{fs::File, sync::Arc, time::SystemTime};

use arrow2::io::parquet::read;
use itertools::Itertools;
use rayon::prelude::{IntoParallelRefMutIterator, ParallelBridge};
use snafu::ResultExt;

use crate::{
    file,
    read::{ArrowChunk, ParquetSchemaInferenceOptions},
};

use crate::stream_reader::read::schema::infer_schema_with_options;
use arrow2::io::parquet::read::ArrayIter;
use rayon::iter::ParallelIterator;

fn local_parquet_read(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> super::Result<Vec<ArrowChunk>> {
    const LOCAL_PROTOCOL: &str = "file://";

    let uri = uri.strip_prefix(LOCAL_PROTOCOL).unwrap_or(uri);

    let mut reader = File::open(uri).with_context(|_| super::InternalIOSnafu {
        path: uri.to_string(),
    })?;
    let metadata = read::read_metadata(&mut reader).with_context(|_| {
        super::UnableToParseMetadataFromLocalFileSnafu {
            path: uri.to_string(),
        }
    })?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = infer_schema_with_options(&metadata, &Some(schema_infer_options.into()))
        .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
            path: uri.to_string(),
        })?;
    let schema = schema.filter(|_index, _field| true);
    let chunk_size = 128 * 1024;
    let expected_rows = metadata.num_rows.min(num_rows.unwrap_or(metadata.num_rows));

    let num_expected_arrays = f32::ceil(expected_rows as f32 / chunk_size as f32) as usize;

    let columns_iters_per_rg = metadata
        .row_groups
        .iter()
        .map(|rg| {
            let single_rg_column_iter = read::read_columns_many(
                &mut reader,
                rg,
                schema.fields.clone(),
                Some(chunk_size),
                None,
                None,
            );

            let single_rg_column_iter = single_rg_column_iter?;
            arrow2::error::Result::Ok(single_rg_column_iter.into_iter().enumerate())
        })
        .flatten_ok();

    let columns_iters_per_rg = columns_iters_per_rg.par_bridge();
    let collected_columns = columns_iters_per_rg.map(|payload: Result<_, _>| {
        let (idx, v) = payload?;
        Ok((idx, v.collect::<Result<Vec<_>, _>>()?))
    });
    let mut all_columns = vec![Vec::with_capacity(num_expected_arrays); schema.fields.len()];
    let all_computed = collected_columns
        .collect::<Result<Vec<_>, _>>()
        .with_context(|_| super::UnableToCreateChunkFromStreamingFileReaderSnafu {
            path: uri.to_string(),
        })?;
    for (idx, v) in all_computed {
        all_columns
            .get_mut(idx)
            .expect("array index during scatter out of index")
            .extend(v);
    }
    Ok(all_columns)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use common_error::DaftResult;

    use crate::stream_reader::local_parquet_read;

    #[test]
    fn test_local_parquet_read() -> DaftResult<()> {
        let file = "/Users/sammy/daft_200MB_lineitem_chunk.RG-2.parquet";
        let start = SystemTime::now();
        let _ = local_parquet_read(file, None, None, None, None, Default::default())?;
        println!("took: {} ms", start.elapsed().unwrap().as_millis());
        let start = SystemTime::now();
        let _ = local_parquet_read(file, None, None, None, None, Default::default())?;
        println!("took: {} ms", start.elapsed().unwrap().as_millis());

        Ok(())
    }
}
