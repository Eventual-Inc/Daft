use std::collections::HashMap;

use common_error::DaftResult;
use daft_core::{datatypes::Utf8Array, series::IntoSeries};
use daft_decoding::inference::infer;
use daft_schema::{dtype::DaftDataType, field::Field, schema::Schema};
use daft_table::Table;

/// Parses hive-style /key=value/ components from a uri.
pub fn parse_hive_partitioning(uri: &str) -> HashMap<&str, &str> {
    let mut equality_pos = 0;
    let mut partition_start = 0;
    let mut valid_partition = true;
    let mut partitions = HashMap::new();
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
                    let key = &uri[partition_start..equality_pos];
                    let value = &uri[equality_pos + 1..idx];
                    partitions.insert(key, value);
                }
                partition_start = idx + 1;
                valid_partition = true;
            }
            _ => (),
        }
    }
    partitions
}

/// Takes hive partition key-value pairs as `partitions`, and the schema of the containing table as
/// `table_schema`, and returns a 1-dimensional table containing the partition keys as columns, and
/// their partition values as the singular row of values.
pub fn hive_partitions_to_1d_table(
    partitions: &HashMap<&str, &str>,
    table_schema: &Schema,
) -> DaftResult<Table> {
    // Take the partition keys and values, and convert them into a 1D table with UTF8 fields.
    let uncasted_fields = partitions
        .keys()
        .map(|&key| Field::new(key, daft_schema::dtype::DataType::Utf8))
        .collect();
    let uncasted_schema = Schema::new(uncasted_fields)?;
    let uncasted_series = partitions
        .iter()
        .map(|(&key, &value)| {
            let arrow_array = arrow2::array::Utf8Array::from_iter_values(std::iter::once(&value));
            let daft_array = Utf8Array::from((key, Box::new(arrow_array)));
            daft_array.into_series()
        })
        .collect::<Vec<_>>();
    let uncast_table = Table::new_unchecked(uncasted_schema, uncasted_series, /*num_rows=*/ 1);

    // Using the given table schema, get the expected schema for the partition fields, and cast the
    // table of UTF8 partition values accordingly into their expected types.
    let partition_fields = table_schema
        .fields
        .clone()
        .into_iter()
        .map(|(_, field)| field)
        .filter(|field| partitions.contains_key(&field.name.as_str()))
        .collect();
    let partition_schema = Schema::new(partition_fields)?;
    // TODO(desmond): There's probably a better way to do this, instead of creating a UTF8 table
    //                then casting it. We should be able to create this table directly.
    let casted_table = uncast_table.cast_to_schema(&partition_schema)?;
    Ok(casted_table)
}

/// Turns hive partition key-value pairs into a schema with the partitions' keys as field names, and
/// inferring field types from the partitions' values. We don't do schema type inference here as the
/// user is expected to provide the schema for hive-partitioned fields.
pub fn hive_partitions_to_schema(partitions: &HashMap<&str, &str>) -> DaftResult<Schema> {
    let partition_fields: Vec<Field> = partitions
        .iter()
        .map(|(&key, &value)| Field::new(key, DaftDataType::from(&infer(value.as_bytes()))))
        .collect();
    Schema::new(partition_fields)
}
