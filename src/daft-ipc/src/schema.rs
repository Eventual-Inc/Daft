use std::sync::Arc;

use arrow_ipc::{
    reader::{StreamReader, read_footer_length},
    root_as_footer_with_opts,
};
use common_error::{DaftError, DaftResult};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef};
use daft_schema::schema::SchemaRef;
use flatbuffers::VerifierOptions;

/// Infer the Daft schema from the first Arrow IPC file at `uri` (full file is downloaded).
pub async fn read_ipc_file_schema(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<SchemaRef> {
    let mut magic_bytes = [0; 10];
    let magic_result = io_client
        .single_url_get(
            uri.to_string(),
            Some(GetRange::Suffix(10)),
            io_stats.clone(),
        )
        .await?
        .bytes()
        .await?;
    magic_bytes[..].copy_from_slice(magic_result.as_ref());
    let footer_len = read_footer_length(magic_bytes).map_err(|_| {
        DaftError::InternalError(format!("Failed to read footer length for file `{uri}`"))
    })?;

    // Read footer
    let footer_data = io_client
        .single_url_get(
            uri.to_string(),
            Some(GetRange::Suffix(footer_len + 10)),
            io_stats,
        )
        .await?
        .bytes()
        .await?;
    let verifier_options = VerifierOptions::default();
    let footer = root_as_footer_with_opts(&verifier_options, &footer_data[..footer_len])
        .map_err(|err| DaftError::ValueError(format!("Unable to get root as footer: {err:?}")))?;

    // Get schema from footer
    let ipc_schema = footer.schema().unwrap();
    if !ipc_schema.endianness().equals_to_target_endianness() {
        return Err(DaftError::ValueError(
            "the endianness of the source system does not match the endianness of the target system.".to_owned()
        ));
    }

    // Convert to Daft schema
    let arrow_schema = arrow_ipc::convert::fb_to_schema(ipc_schema);
    Ok(Arc::new(arrow_schema.as_ref().try_into()?))
}

pub async fn read_ipc_stream_schema(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<SchemaRef> {
    let file = io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?;

    let GetResult::File(file) = file else {
        return Err(DaftError::ValueError(format!(
            "Expected a file for Arrow IPC stream, got a stream for `{uri}`"
        )));
    };

    let file = std::fs::File::open(file.path)?;
    let reader = StreamReader::try_new(file, None)?;

    Ok(Arc::new(reader.schema().as_ref().try_into()?))
}
