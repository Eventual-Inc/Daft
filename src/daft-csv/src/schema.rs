use std::collections::HashSet;

/// Merges two Arrow2 schemas
pub fn merge_schema(
    headers: &[String],
    column_types: &mut [HashSet<daft_arrow::datatypes::DataType>],
) -> Vec<daft_arrow::datatypes::Field> {
    headers
        .iter()
        .zip(column_types.iter_mut())
        .map(|(field_name, possibilities)| merge_fields(field_name, possibilities))
        .collect()
}

fn merge_fields(
    field_name: &str,
    possibilities: &mut HashSet<daft_arrow::datatypes::DataType>,
) -> daft_arrow::datatypes::Field {
    use daft_arrow::datatypes::DataType;

    if possibilities.len() > 1 {
        // Drop nulls from possibilities.
        possibilities.remove(&DataType::Null);
    }
    // determine data type based on possible types
    // if there are incompatible types, use DataType::Utf8
    let data_type = match possibilities.len() {
        1 => possibilities.drain().next().unwrap(),
        2 => {
            if possibilities.contains(&DataType::Int64)
                && possibilities.contains(&DataType::Float64)
            {
                // we have an integer and double, fall down to double
                DataType::Float64
            } else {
                // default to Utf8 for conflicting datatypes (e.g bool and int)
                DataType::Utf8
            }
        }
        _ => DataType::Utf8,
    };
    daft_arrow::datatypes::Field::new(field_name, data_type, true)
}
