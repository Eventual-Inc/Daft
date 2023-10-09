use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow2::{
    datatypes::Field,
    io::csv::read_async::{
        deserialize_batch, deserialize_column, infer, infer_schema, read_rows, AsyncReaderBuilder,
        ByteRecord,
    },
};
use async_compat::CompatExt;
use common_error::DaftResult;
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{get_runtime, GetResult, IOClient};
use daft_table::Table;
use futures::{io::Cursor, AsyncRead, AsyncSeek};
use tokio::fs::File;

#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    uri: &str,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
    multithreaded_io: bool,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_csv_single(
            uri,
            column_names,
            include_columns,
            num_rows,
            has_header,
            delimiter,
            io_client,
        )
        .await
    })
}

async fn read_csv_single(
    uri: &str,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    match io_client.single_url_get(uri.to_string(), None).await? {
        GetResult::File(file) => {
            read_csv_single_from_reader(
                File::open(file.path).await?.compat(),
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
            )
            .await
        }
        result @ GetResult::Stream(..) => {
            // TODO(Clark): Enable streaming remote reads by wrapping the BoxStream in a buffered stream that's
            // (1) sync and (2) seekable.
            read_csv_single_from_reader(
                Cursor::new(result.bytes().await?),
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
            )
            .await
        }
    }
}

async fn read_csv_single_from_reader<R>(
    reader: R,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
) -> DaftResult<Table>
where
    R: AsyncRead + AsyncSeek + Unpin + Sync + Send,
{
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter.unwrap_or(b','))
        .create_reader(reader);
    let (mut fields, _) = infer_schema(&mut reader, None, has_header, &infer).await?;
    if let Some(column_names) = column_names {
        fields = fields
            .into_iter()
            .zip(column_names.iter())
            .map(|(field, name)| {
                Field::new(*name, field.data_type, field.is_nullable).with_metadata(field.metadata)
            })
            .collect();
    }
    let field_name_to_idx = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.name.as_ref(), idx))
        .collect::<HashMap<&str, usize>>();
    let projection_indices = include_columns.as_ref().map(|cols| {
        cols.iter()
            .map(|c| field_name_to_idx[c])
            .collect::<Vec<_>>()
    });
    let num_rows = num_rows.unwrap_or(usize::MAX);
    // TODO(Clark): Make batch size configurable.
    // TODO(Clark): Get estimated average row size in bytes during schema inference and use it to:
    //   1. Set a reasonable batch size.
    //   2. Preallocate per-column batch vecs.
    //   3. Preallocate column Arrow array buffers.
    let batch_size = 1024.min(num_rows);
    // TODO(Clark): Instead of allocating an array-per-column-batch and concatenating at the end,
    // progressively grow a single array per column (with the above preallocation based on estimated
    // number of rows).
    let mut column_arrays = vec![
        vec![];
        projection_indices
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(fields.len())
    ];
    let mut buffer = vec![ByteRecord::with_capacity(0, fields.len()); batch_size];
    let mut rows = buffer.as_mut_slice();
    // Number of rows read in last read.
    let mut rows_read = 1;
    // Total number of rows read across all reads.
    let mut total_rows_read = 0;
    while rows_read > 0 && total_rows_read < num_rows {
        if rows.len() > num_rows - total_rows_read {
            // If we need to read less than the entire row buffer, truncate the buffer to the number
            // of rows that we actually want to read.
            rows = &mut rows[..num_rows - total_rows_read + 1]
        }
        rows_read = read_rows(&mut reader, 0, rows).await?;
        total_rows_read += rows_read;
        // TODO(Clark): Parallelize column deserialization over a rayon threadpool.
        for (idx, array) in deserialize_batch(
            &rows[..rows_read],
            &fields,
            projection_indices.as_deref(),
            0,
            deserialize_column,
        )?
        .into_arrays()
        .into_iter()
        .enumerate()
        {
            column_arrays[idx].push(array);
        }
    }
    if let Some(include_columns) = include_columns {
        // Truncate fields to only contain projected columns.
        let include_columns: HashSet<&str> = include_columns.into_iter().collect();
        fields.retain(|f| include_columns.contains(f.name.as_str()))
    }
    let columns_series = column_arrays
        .into_iter()
        .zip(fields.iter())
        .map(|(mut arrays, field)| {
            let array = if arrays.len() > 1 {
                // Concatenate all array chunks.
                let unboxed_arrays = arrays.iter().map(Box::as_ref).collect::<Vec<_>>();
                arrow2::compute::concatenate::concatenate(unboxed_arrays.as_slice())?
            } else {
                // Return single array chunk directly.
                arrays.pop().unwrap()
            };
            Series::try_from((field.name.as_ref(), cast_array_for_daft_if_needed(array)))
        })
        .collect::<DaftResult<Vec<Series>>>()?;
    let schema: arrow2::datatypes::Schema = fields.into();
    let daft_schema = Schema::try_from(&schema)?;
    Table::new(daft_schema, columns_series)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;

    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_io::{IOClient, IOConfig};

    use super::read_csv;

    #[test]
    fn test_csv_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file, None, None, None, true, None, io_client, true)?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_larger_than_batch_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/medium.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file, None, None, None, true, None, io_client, true)?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_limit() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file, None, None, Some(10), true, None, io_client, true)?;
        assert_eq!(table.len(), 10);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            None,
            Some(vec!["b"]),
            None,
            true,
            None,
            io_client,
            true,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![Field::new("b", DataType::Utf8)])?.into(),
        );

        Ok(())
    }
}
