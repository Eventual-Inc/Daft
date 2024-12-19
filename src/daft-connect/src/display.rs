use std::fmt::Write;

use daft_core::prelude::*;

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

impl SparkDisplay for Field {
    fn repr_spark_string(&self) -> String {
        // Fields on their own need context (indentation) to print nicely.
        // For a standalone Field, we might choose zero indentation or provide a helper method.
        // Here we choose zero indentation since it's ambiguous outside a schema:
        let mut output = String::new();
        write_field(&mut output, &self.name, &self.dtype, 0).unwrap();
        output
    }
}

impl SparkDisplay for DataType {
    fn repr_spark_string(&self) -> String {
        type_to_str(self).to_string()
    }
}

// Private helpers to mimic the original indentation style and recursive printing:

fn write_field(
    w: &mut String,
    field_name: &str,
    dtype: &DataType,
    level: usize,
) -> eyre::Result<()> {
    /// All daft fields are nullable.
    const NULLABLE: bool = true;

    let indent = make_indent(level);

    let dtype_str = type_to_str(dtype);
    writeln!(
        w,
        "{indent}{field_name}: {dtype_str} (nullable = {NULLABLE})"
    )?;

    if let DataType::Struct(fields) = dtype {
        for field in fields {
            write_field(w, &field.name, &field.dtype, level + 1)?;
        }
    }

    Ok(())
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

fn type_to_str(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Null => "null",
        DataType::Boolean => "boolean",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer",
        DataType::Float32 | DataType::Float64 => "double",
        DataType::Decimal128(_, _) => "decimal",
        DataType::Timestamp(_, _) => "timestamp",
        DataType::Date => "date",
        DataType::Time(_) => "time",
        DataType::Duration(_) => "duration",
        DataType::Interval => "interval",
        DataType::Binary => "binary",
        DataType::FixedSizeBinary(_) => "fixed_size_binary",
        DataType::Utf8 => "string",
        DataType::FixedSizeList(_, _) | DataType::List(_) => "array",
        DataType::Struct(_) => "struct",
        DataType::Map { .. } => "map",
        DataType::Extension(_, _, _) => "extension",
        DataType::Embedding(_, _) => "embedding",
        DataType::Image(_) => "image",
        DataType::FixedShapeImage(_, _, _) => "fixed_shape_image",
        DataType::Tensor(_) => "tensor",
        DataType::FixedShapeTensor(_, _) => "fixed_shape_tensor",
        DataType::SparseTensor(_) => "sparse_tensor",
        DataType::FixedShapeSparseTensor(_, _) => "fixed_shape_sparse_tensor",
        #[cfg(feature = "python")]
        DataType::Python => "python_object",
        DataType::Unknown => "unknown",
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

        let output = schema.repr_spark_string();
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

        let output = schema.repr_spark_string();
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

        let output = schema.repr_spark_string();
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
 |-- empty_list: array (nullable = true)
";
        assert_eq!(output, expected);
        Ok(())
    }
}
