use std::sync::Arc;

use daft_schema::schema::SchemaRef;

use crate::{AvroError, Result};

/// Read the Avro schema from an OCF file header without reading data.
pub async fn read_avro_schema(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
) -> Result<SchemaRef> {
    use daft_io::GetResult;
    use futures::StreamExt;

    let get_result = io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await
        .map_err(|e| AvroError::IOError { source: e })?;

    let bytes = match get_result {
        GetResult::File(file) => {
            tokio::fs::read(file.path)
                .await
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!("Failed to read Avro schema file: {}", e),
                })?
        }
        GetResult::Stream(stream, ..) => {
            let mut data = Vec::new();
            futures::pin_mut!(stream);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| AvroError::IOError { source: e })?;
                data.extend_from_slice(&chunk);
            }
            data
        }
    };

    let cursor = std::io::Cursor::new(bytes);
    let reader = arrow_avro::reader::ReaderBuilder::new()
        .build(std::io::BufReader::new(cursor))
        .map_err(|e| AvroError::ReadError {
            message: format!("Failed to create Avro reader for schema: {}", e),
        })?;

    let arrow_schema = reader.schema();
    let daft_schema =
        daft_schema::schema::Schema::try_from(arrow_schema.as_ref()).map_err(|e| {
            AvroError::SchemaConversionError {
                message: format!("Failed to convert Arrow schema to Daft schema: {}", e),
            }
        })?;
    Ok(Arc::new(daft_schema))
}
