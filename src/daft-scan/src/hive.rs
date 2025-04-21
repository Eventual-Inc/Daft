use arrow2::datatypes::DataType;
use common_error::DaftResult;
use daft_core::{datatypes::Utf8Array, series::Series};
use daft_decoding::inference::infer;
use daft_schema::{dtype::DaftDataType, field::Field, schema::Schema};
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
                    let key =
                        urlencoding::decode(&uri[partition_start..equality_pos])?.into_owned();
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
pub fn hive_partitions_to_series(
    partitions: &IndexMap<String, String>,
    table_schema: &Schema,
) -> DaftResult<Vec<Series>> {
    partitions
        .iter()
        .filter_map(|(key, value)| {
            if table_schema.has_field(key) {
                let target_dtype = &table_schema.get_field(key).unwrap().dtype;
                if value.is_empty() {
                    Some(Ok(Series::full_null(key, target_dtype, 1)))
                } else {
                    let daft_utf8_array = Utf8Array::from_values(key, std::iter::once(&value));
                    Some(daft_utf8_array.cast(target_dtype))
                }
            } else {
                None
            }
        })
        .collect::<DaftResult<Vec<_>>>()
}

/// Turns hive partition key-value pairs into a vector of fields with the partitions' keys as field names, and
/// inferring field types from the partitions' values.
pub fn hive_partitions_to_fields(partitions: &IndexMap<String, String>) -> Vec<Field> {
    partitions
        .iter()
        .map(|(key, value)| {
            let inferred_type = infer(value.as_bytes());
            let inferred_type = if inferred_type == DataType::Null {
                // If we got a null partition value, don't assume that the column will be Null.
                // The user should provide a schema for potentially null columns.
                DataType::LargeUtf8
            } else {
                inferred_type
            };
            Field::new(key, DaftDataType::from(&inferred_type))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    // use std::assert_matches::assert_matches;

    #[test]
    fn test_hive_basic_partitions() {
        let uri = "s3://bucket/year=2024/normal-string/month=03/day=15/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
        assert_eq!(partitions.get("day"), Some(&"15".to_string()));
    }

    #[test]
    fn test_hive_url_encoded_values() {
        let uri = "s3://bucket/country=United%20States/city=New%20York/data.csv";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(
            partitions.get("country"),
            Some(&"United States".to_string())
        );
        assert_eq!(partitions.get("city"), Some(&"New York".to_string()));
    }

    #[test]
    fn test_hive_default_partition() {
        let uri = "/year=2024/region=__HIVE_DEFAULT_PARTITION__/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("region"), Some(&"".to_string()));
    }

    #[test]
    fn test_hive_empty_uri() {
        let uri = "";
        let partitions = parse_hive_partitioning(uri).unwrap();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_hive_no_partitions() {
        let uri = "s3://bucket/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_hive_invalid_partition_format() {
        let uri = "s3://bucket/year=2024/invalid==partition/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        // Should only parse the valid partition.
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
    }

    #[test]
    fn test_hive_get_parameters() {
        let uri = "s3://bucket/year=2024/month=03/file.parquet?query=value";
        let partitions = parse_hive_partitioning(uri).unwrap();

        // Should only parse up to the '?'.
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
    }

    #[test]
    fn test_hive_windows_style_paths() {
        let uri = "C:\\Documents\\year=2024\\month=03\\data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
    }

    #[test]
    fn test_hive_mixed_separators() {
        let uri = "C:\\Documents/year=2024\\month=03/day=15\\data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
        assert_eq!(partitions.get("day"), Some(&"15".to_string()));
    }

    #[test]
    fn test_hive_newline_termination() {
        let uri = "s3://bucket/year=2024/month=03/\nday=15/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        // Should only parse up to the newline.
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
    }

    #[test]
    fn test_hive_special_characters() {
        let uri = "s3://bucket/region=North%40America/type%3Ddata=csv%2Bextra/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions.get("region"), Some(&"North@America".to_string()));
        assert_eq!(partitions.get("type=data"), Some(&"csv+extra".to_string()));
    }

    #[test]
    fn test_hive_unicode_characters() {
        let uri = "s3://bucket/country=%F0%9F%87%BA%F0%9F%87%B8/city=%E5%8C%97%E4%BA%AC/data.csv";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.get("country"), Some(&"🇺🇸".to_string()));
        assert_eq!(partitions.get("city"), Some(&"北京".to_string()));
    }

    #[test]
    fn test_hive_consecutive_separators() {
        let uri = "s3://bucket/year=2024//month=03///day=15////data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
        assert_eq!(partitions.get("day"), Some(&"15".to_string()));
    }

    #[test]
    fn test_hive_mixed_consecutive_separators() {
        let uri = "s3://bucket/year=2024//\\month=03\\/\\day=15/\\//data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions.get("year"), Some(&"2024".to_string()));
        assert_eq!(partitions.get("month"), Some(&"03".to_string()));
        assert_eq!(partitions.get("day"), Some(&"15".to_string()));
    }

    #[test]
    fn test_hive_empty_key_value() {
        let uri = "s3://bucket/empty_key=/=empty_value/another=/data.parquet";
        let partitions = parse_hive_partitioning(uri).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions.get("empty_key"), Some(&"".to_string()));
        assert_eq!(partitions.get("another"), Some(&"".to_string()));
    }
}
