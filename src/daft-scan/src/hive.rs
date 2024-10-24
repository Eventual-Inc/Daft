use std::sync::Arc;

use common_error::DaftResult;
use daft_core::datatypes::Utf8Array;
use daft_decoding::inference::infer;
use daft_schema::{dtype::DaftDataType, field::Field, schema::Schema};
use daft_table::Table;
use indexmap::IndexMap;

const DEFAULT_HIVE_PARTITION_NAME: &str = "__HIVE_DEFAULT_PARTITION__";

/// Parses hive-style /key=value/ components from a uri.
pub fn parse_hive_partitioning(uri: &str) -> DaftResult<IndexMap<String, String>> {
    let mut equality_pos = 0;
    let mut partition_start = 0;
    let mut valid_partition = true;
    let mut partitions = IndexMap::new();
    // Loops through the uri looking for valid partitions. Although we consume partitions only when
    // encountering a slash separator, we never need to grab a partition key-value pair from the end
    // of the uri because uri's are expected to end in either a filename, or in GET parameters.
    for (idx, c) in uri.char_indices() {
        match c {
            // A '?' char denotes the start of GET parameters, so stop parsing hive partitions.
            // We also ban '\n' for hive partitions, given all the edge cases that can arise.
            '?' | '\n' => break,
            // A '=' char denotes that we've finished reading the partition's key, and we're now
            // reading the partition's value.
            '=' => {
                // If we see more than one '=' in the partition, it is not a valid partition.
                if equality_pos > partition_start {
                    valid_partition = false;
                }
                equality_pos = idx;
            }
            // A separator char denotes the start of a new partition.
            '\\' | '/' => {
                if valid_partition && equality_pos > partition_start {
                    let key = uri[partition_start..equality_pos].to_string();
                    let value = {
                        // Decode the potentially url-encoded string.
                        let value = urlencoding::decode(&uri[equality_pos + 1..idx])?.into_owned();
                        // In Hive, __HIVE_DEFAULT_PARTITION__ is used to represent a null value.
                        if value == DEFAULT_HIVE_PARTITION_NAME {
                            String::new()
                        } else {
                            value
                        }
                    };
                    partitions.insert(key, value);
                }
                partition_start = idx + 1;
                valid_partition = true;
            }
            _ => (),
        }
    }
    Ok(partitions)
}

/// Takes hive partition key-value pairs as `partitions`, and the schema of the containing table as
/// `table_schema`, and returns a 1-dimensional table containing the partition keys as columns, and
/// their partition values as the singular row of values.
pub fn hive_partitions_to_1d_table(
    partitions: &IndexMap<String, String>,
    table_schema: &Schema,
) -> DaftResult<Table> {
    let partition_series = partitions
        .iter()
        .filter_map(|(key, value)| {
            if table_schema.fields.contains_key(key) {
                let daft_utf8_array = Utf8Array::from_values(key, std::iter::once(&value));
                let target_dtype = &table_schema.fields.get(key).unwrap().dtype;
                Some(daft_utf8_array.cast(target_dtype))
            } else {
                None
            }
        })
        .collect::<DaftResult<Vec<_>>>()?;
    let partition_fields = table_schema
        .fields
        .clone()
        .into_iter()
        .map(|(_, field)| field)
        .filter(|field| partitions.contains_key(&field.name))
        .collect();
    let partition_schema = Schema::new(partition_fields)?;
    Ok(Table::new_unchecked(
        Arc::new(partition_schema),
        partition_series,
        1,
    ))
}

/// Turns hive partition key-value pairs into a schema with the partitions' keys as field names, and
/// inferring field types from the partitions' values. We don't do schema type inference here as the
/// user is expected to provide the schema for hive-partitioned fields.
pub fn hive_partitions_to_schema(partitions: &IndexMap<String, String>) -> DaftResult<Schema> {
    let partition_fields: Vec<Field> = partitions
        .iter()
        .map(|(key, value)| Field::new(key, DaftDataType::from(&infer(value.as_bytes()))))
        .collect();
    Schema::new(partition_fields)
}
