use std::sync::Arc;

use arrow_avro::reader::ReaderBuilder;
use daft_core::datatypes::{DataType, Field as DaftField};
use daft_schema::schema::{Schema as DaftSchema, SchemaRef};

use crate::{AvroError, Result};

/// Convert an Arrow DataType to a Daft DataType.
pub fn arrow_type_to_daft_type(arrow_type: &arrow_schema::DataType) -> Result<DataType> {
    use arrow_schema::DataType as AT;

    match arrow_type {
        AT::Null => Ok(DataType::Null),
        AT::Boolean => Ok(DataType::Boolean),
        AT::Int8 => Ok(DataType::Int8),
        AT::Int16 => Ok(DataType::Int16),
        AT::Int32 => Ok(DataType::Int32),
        AT::Int64 => Ok(DataType::Int64),
        AT::UInt8 => Ok(DataType::UInt8),
        AT::UInt16 => Ok(DataType::UInt16),
        AT::UInt32 => Ok(DataType::UInt32),
        AT::UInt64 => Ok(DataType::UInt64),
        AT::Float16 => Err(AvroError::UnsupportedType {
            type_name: "Float16".to_string(),
        }),
        AT::Float32 => Ok(DataType::Float32),
        AT::Float64 => Ok(DataType::Float64),
        AT::Utf8 | AT::LargeUtf8 => Ok(DataType::Utf8),
        AT::Binary | AT::LargeBinary => Ok(DataType::Binary),
        AT::FixedSizeBinary(size) => Ok(DataType::FixedSizeBinary(*size as usize)),
        AT::Date32 => Ok(DataType::Date),
        AT::List(f) | AT::LargeList(f) => {
            let inner = arrow_type_to_daft_type(f.data_type())?;
            Ok(DataType::List(Box::new(inner)))
        }
        AT::Struct(fields) => {
            let daft_fields: Vec<DaftField> = fields
                .iter()
                .map(|f| {
                    let dtype = arrow_type_to_daft_type(f.data_type())?;
                    Ok(DaftField::new(f.name().clone(), dtype))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(daft_fields))
        }
        AT::Map(f, _) => {
            let entries = match f.data_type() {
                AT::Struct(fields) => {
                    let value_type =
                        fields.iter().find(|f| f.name() == "value").ok_or_else(|| {
                            AvroError::SchemaConversionError {
                                message: "Map entry missing 'value' field".to_string(),
                            }
                        })?;
                    arrow_type_to_daft_type(value_type.data_type())?
                }
                _ => {
                    return Err(AvroError::SchemaConversionError {
                        message: format!("Unexpected Map entry type: {:?}", f.data_type()),
                    });
                }
            };
            Ok(DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(entries),
            })
        }
        other => Err(AvroError::UnsupportedType {
            type_name: format!("{:?}", other),
        }),
    }
}

/// Read the Avro schema from an OCF file header without reading data.
pub async fn read_avro_schema(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
) -> Result<SchemaRef> {
    let get_result = io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await
        .map_err(|e| AvroError::IOError { source: e })?;

    use daft_io::GetResult;
    let bytes = match get_result {
        GetResult::File(file) => {
            tokio::fs::read(file.path)
                .await
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!("Failed to read Avro schema file: {}", e),
                })?
        }
        GetResult::Stream(stream, ..) => {
            use futures::StreamExt;
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
    let reader = ReaderBuilder::new()
        .build(std::io::BufReader::new(cursor))
        .map_err(|e| AvroError::ReadError {
            message: format!("Failed to create Avro reader for schema: {}", e),
        })?;

    let arrow_schema = reader.schema();
    let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;
    Ok(Arc::new(daft_schema))
}

fn arrow_schema_to_daft_schema(arrow_schema: &arrow_schema::Schema) -> Result<DaftSchema> {
    let fields: Vec<DaftField> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            let dtype = arrow_type_to_daft_type(f.data_type())?;
            Ok(DaftField::new(f.name().clone(), dtype))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DaftSchema::new(fields))
}
