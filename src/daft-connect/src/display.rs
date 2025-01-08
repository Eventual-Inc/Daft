use std::fmt::Write;

use daft_core::prelude::*;

// note: right now this is only implemented for Schema, but we'll want to extend this for our dataframe output, and the plan repr.
pub trait SparkDisplay {
    fn repr_spark_string(&self) -> String;
}

impl SparkDisplay for Schema {
    fn repr_spark_string(&self) -> String {
        let mut output = String::new();
        // Start with root
        writeln!(&mut output, "root").unwrap();

        // Print each top-level field with indentation level 1
        for (name, field) in &self.fields {
            // We'll rely on a helper function that knows how to print a field with given indentation
            write_field(&mut output, name, &field.dtype, 1).unwrap();
        }
        output
    }
}

// Private helpers to mimic the original indentation style and recursive printing:
fn write_field(
    w: &mut String,
    field_name: &str,
    dtype: &DataType,
    level: usize,
) -> eyre::Result<()> {
    fn write_field_inner(
        w: &mut String,
        field_name: &str,
        dtype: &DataType,
        level: usize,
        is_list: bool,
    ) -> eyre::Result<()> {
        let indent = make_indent(level);

        let dtype_str = type_to_string(dtype);

        writeln!(
            w,
            "{indent}{field_name}: {dtype_str} ({nullable} = true)",
            // for some reason, spark prints "containsNulls" instead of "nullable" for lists
            nullable = if is_list { "containsNulls" } else { "nullable" }
        )?;

        // handle nested dtypes
        match dtype {
            DataType::List(inner) => {
                write_field_inner(w, "element", inner, level + 1, true)?;
            }
            DataType::FixedSizeList(inner, _) => {
                write_field_inner(w, "element", inner, level + 1, true)?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    write_field_inner(w, &field.name, &field.dtype, level + 1, false)?;
                }
            }
            _ => {}
        };
        Ok(())
    }

    write_field_inner(w, field_name, dtype, level, false)
}

// This helper creates indentation of the form:
// level=1: " |-- "
// level=2: " |    |-- "
// and so forth.
fn make_indent(level: usize) -> String {
    if level == 0 {
        // If top-level (i.e., a bare field not in a schema), just return empty.
        String::new()
    } else if level == 1 {
        " |-- ".to_string()
    } else {
        format!(" |{}-- ", "    |".repeat(level - 1))
    }
}

fn type_to_string(dtype: &DataType) -> String {
    match dtype {
        DataType::Null => "null".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "byte".to_string(),
        DataType::Int16 => "short".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "long".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time(_) => "time".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Interval => "interval".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::FixedSizeBinary(_) => "arrow.fixed_size_binary".to_string(),
        DataType::Utf8 => "string".to_string(),
        DataType::FixedSizeList(_, _) => "arrow.fixed_size_list".to_string(),
        DataType::List(_) => "array".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        DataType::Map { .. } => "map".to_string(),
        DataType::Extension(_, _, _) => "daft.extension".to_string(),
        DataType::Embedding(_, _) => "daft.embedding".to_string(),
        DataType::Image(_) => "daft.image".to_string(),
        DataType::FixedShapeImage(_, _, _) => "daft.fixed_shape_image".to_string(),
        DataType::Tensor(_) => "daft.tensor".to_string(),
        DataType::FixedShapeTensor(_, _) => "daft.fixed_shape_tensor".to_string(),
        DataType::SparseTensor(_) => "daft.sparse_tensor".to_string(),
        DataType::FixedShapeSparseTensor(_, _) => "daft.fixed_shape_sparse_tensor".to_string(),
        #[cfg(feature = "python")]
        DataType::Python => "daft.python".to_string(),
        DataType::Unknown => "unknown".to_string(),
        DataType::UInt8 => "arrow.uint8".to_string(),
        DataType::UInt16 => "arrow.uint16".to_string(),
        DataType::UInt32 => "arrow.uint32".to_string(),
        DataType::UInt64 => "arrow.uint64".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_schema() -> eyre::Result<()> {
        let schema = Schema::empty();
        let output = schema.repr_spark_string();
        let expected = "root\n";
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn test_single_field_schema() -> eyre::Result<()> {
        let mut fields = Vec::new();
        fields.push(Field::new("step", DataType::Int32));
        let schema = Schema::new(fields)?;
        let output = schema.repr_spark_string();
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
        let output = schema.repr_spark_string();
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- parent: struct (nullable = true)
 |    |-- inner1: string (nullable = true)
 |    |-- inner2: float (nullable = true)
 |-- count: long (nullable = true)
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- top: struct (nullable = true)
 |    |-- mid1: byte (nullable = true)
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- ints: array (nullable = true)
 |    |-- element: short (containsNulls = true)
 |-- floats: arrow.fixed_size_list (nullable = true)
 |    |-- element: float (containsNulls = true)
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

        let output = schema.repr_spark_string();
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- ext_field: daft.extension (nullable = true)
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- record: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- values: array (nullable = true)
 |    |    |-- element: long (containsNulls = true)
 |    |-- nested: struct (nullable = true)
 |    |    |-- sub_list: array (nullable = true)
 |    |    |    |-- element: string (containsNulls = true)
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
        let output = schema.repr_spark_string();
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

        let output = schema.repr_spark_string();
        let expected = "\
root
 |-- empty_list: arrow.fixed_size_list (nullable = true)
 |    |-- element: byte (containsNulls = true)
";
        assert_eq!(output, expected);
        Ok(())
    }
}
