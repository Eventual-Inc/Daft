use std::fmt::Write;

use daft_core::prelude::*;

pub fn to_tree_string(schema: &Schema) -> eyre::Result<String> {
    let mut output = String::new();
    // Start with root
    writeln!(&mut output, "root")?;
    // Now print each top-level field
    for (name, field) in &schema.fields {
        print_field(&mut output, name, &field.dtype, /*nullable*/ true, 1)?;
    }
    Ok(output)
}

// A helper function to print a field at a given level of indentation.
// level=1 means a single " |-- " prefix, level=2 means
// " |    |-- " and so on, mimicking Spark's indentation style.
// A helper function to print a field at a given level of indentation.
fn print_field(
    w: &mut String,
    field_name: &str,
    dtype: &DataType,
    nullable: bool,
    level: usize,
) -> eyre::Result<()> {
    let indent = if level == 1 {
        " |-- ".to_string()
    } else {
        format!(" |{}-- ", "    |".repeat(level - 1))
    };

    let dtype_str = type_to_string(dtype);
    writeln!(
        w,
        "{}{}: {} (nullable = {})",
        indent, field_name, dtype_str, nullable
    )?;

    if let DataType::Struct(fields) = dtype {
        for field in fields {
            print_field(w, &field.name, &field.dtype, true, level + 1)?;
        }
    }

    Ok(())
}

fn type_to_string(dtype: &DataType) -> String {
    // We want a nice, human-readable type string.
    // Spark generally prints something like "integer", "string", etc.
    // We'll follow a similar style here:
    match dtype {
        DataType::Null => "null".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer".to_string(), // Spark doesn't differentiate sizes
        DataType::Float32 | DataType::Float64 => "double".to_string(), // Spark calls all floats double for printing
        DataType::Decimal128(_, _) => "decimal".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time(_) => "time".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Interval => "interval".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::FixedSizeBinary(_) => "fixed_size_binary".to_string(),
        DataType::Utf8 => "string".to_string(),
        DataType::FixedSizeList(_, _) => "array".to_string(), // Spark calls them arrays
        DataType::List(_) => "array".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        DataType::Map { .. } => "map".to_string(),
        DataType::Extension(_, _, _) => "extension".to_string(),
        DataType::Embedding(_, _) => "embedding".to_string(),
        DataType::Image(_) => "image".to_string(),
        DataType::FixedShapeImage(_, _, _) => "fixed_shape_image".to_string(),
        DataType::Tensor(_) => "tensor".to_string(),
        DataType::FixedShapeTensor(_, _) => "fixed_shape_tensor".to_string(),
        DataType::SparseTensor(_) => "sparse_tensor".to_string(),
        DataType::FixedShapeSparseTensor(_, _) => "fixed_shape_sparse_tensor".to_string(),
        #[cfg(feature = "python")]
        DataType::Python => "python_object".to_string(),
        DataType::Unknown => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use super::*;

    #[test]
    fn test_empty_schema() -> eyre::Result<()> {
        let schema = Schema {
            fields: IndexMap::new(),
        };
        let output = to_tree_string(&schema)?;
        let expected = "root\n";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_single_field_schema() -> eyre::Result<()> {
        let mut fields = Vec::new();
        fields.push(Field::new("step", DataType::Int32));
        let schema = Schema::new(fields)?;
        let output = to_tree_string(&schema)?;
        let expected = "root\n |-- step: integer (nullable = true)\n";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_simple_fields() -> eyre::Result<()> {
        let mut fields = Vec::new();
        fields.push(Field::new("step", DataType::Int32));
        fields.push(Field::new("type", DataType::Utf8));
        fields.push(Field::new("amount", DataType::Float64));
        let schema = Schema::new(fields)?;
        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- step: integer (nullable = true)
 |-- type: string (nullable = true)
 |-- amount: double (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_struct_field() -> eyre::Result<()> {
        // Create a schema with a struct field
        let inner_fields = vec![
            Field::new("inner1", DataType::Utf8),
            Field::new("inner2", DataType::Float32),
        ];
        let struct_dtype = DataType::Struct(inner_fields);

        let mut fields = Vec::new();
        fields.push(Field::new("parent", struct_dtype));
        fields.push(Field::new("count", DataType::Int64));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- parent: struct (nullable = true)
 |    |-- inner1: string (nullable = true)
 |    |-- inner2: double (nullable = true)
 |-- count: integer (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_nested_struct_in_struct() -> eyre::Result<()> {
        let inner_struct = DataType::Struct(vec![
            Field::new("deep", DataType::Boolean),
            Field::new("deeper", DataType::Utf8),
        ]);
        let mid_struct = DataType::Struct(vec![
            Field::new("mid1", DataType::Int8),
            Field::new("nested", inner_struct),
        ]);

        let mut fields = Vec::new();
        fields.push(Field::new("top", mid_struct));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- top: struct (nullable = true)
 |    |-- mid1: integer (nullable = true)
 |    |-- nested: struct (nullable = true)
 |    |    |-- deep: boolean (nullable = true)
 |    |    |-- deeper: string (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_list_fields() -> eyre::Result<()> {
        let list_of_int = DataType::List(Box::new(DataType::Int16));
        let fixed_list_of_floats = DataType::FixedSizeList(Box::new(DataType::Float32), 3);

        let mut fields = Vec::new();
        fields.push(Field::new("ints", list_of_int));
        fields.push(Field::new("floats", fixed_list_of_floats));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- ints: array (nullable = true)
 |-- floats: array (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_map_field() -> eyre::Result<()> {
        let map_type = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int32),
        };

        let mut fields = Vec::new();
        fields.push(Field::new("m", map_type));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        // Spark-like print doesn't show the internal "entries" struct by name, but we do show it as "struct":
        let expected = "\
root
 |-- m: map (nullable = true)
";
        // Note: If you decide to recurse into Map children (currently we do not), you'd see something like:
        //  |    |-- key: string (nullable = true)
        //  |    |-- value: integer (nullable = true)
        // If you update the code to print the internals of a map, update the test accordingly.
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_extension_type() -> eyre::Result<()> {
        let extension_type =
            DataType::Extension("some_ext_type".to_string(), Box::new(DataType::Int32), None);

        let mut fields = Vec::new();
        fields.push(Field::new("ext_field", extension_type));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- ext_field: extension (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_complex_nested_schema() -> eyre::Result<()> {
        // A very nested schema to test indentation and various types together
        let struct_inner = DataType::Struct(vec![
            Field::new("sub_list", DataType::List(Box::new(DataType::Utf8))),
            Field::new(
                "sub_struct",
                DataType::Struct(vec![
                    Field::new("a", DataType::Int32),
                    Field::new("b", DataType::Float64),
                ]),
            ),
        ]);

        let main_fields = vec![
            Field::new("name", DataType::Utf8),
            Field::new("values", DataType::List(Box::new(DataType::Int64))),
            Field::new("nested", struct_inner),
        ];

        let mut fields = Vec::new();
        fields.push(Field::new("record", DataType::Struct(main_fields)));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- record: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- values: array (nullable = true)
 |    |-- nested: struct (nullable = true)
 |    |    |-- sub_list: array (nullable = true)
 |    |    |-- sub_struct: struct (nullable = true)
 |    |    |    |-- a: integer (nullable = true)
 |    |    |    |-- b: double (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_field_name_special_chars() -> eyre::Result<()> {
        // Field with spaces and special characters
        let mut fields = Vec::new();
        fields.push(Field::new("weird field@!#", DataType::Utf8));
        let schema = Schema::new(fields)?;
        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- weird field@!#: string (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_zero_sized_fixed_list() -> eyre::Result<()> {
        // Although unusual, test a fixed size list with size=0
        let zero_sized_list = DataType::FixedSizeList(Box::new(DataType::Int8), 0);
        let mut fields = Vec::new();
        fields.push(Field::new("empty_list", zero_sized_list));
        let schema = Schema::new(fields)?;

        let output = to_tree_string(&schema)?;
        let expected = "\
root
 |-- empty_list: array (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }
}
