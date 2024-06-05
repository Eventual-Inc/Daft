use arrow2::datatypes::{DataType, Field, Metadata, Schema};

fn main() {
    // two data types (logical types)
    let type1_ = DataType::Date32;
    let type2_ = DataType::Int32;

    // two fields (columns)
    let field1 = Field::new("c1", type1_, true);
    let field2 = Field::new("c2", type2_, true);

    // which can contain extra metadata:
    let mut metadata = Metadata::new();
    metadata.insert(
        "Office Space".to_string(),
        "Deals with real issues in the workplace.".to_string(),
    );
    let field1 = field1.with_metadata(metadata);

    // a schema (a table)
    let schema = Schema::from(vec![field1, field2]);

    assert_eq!(schema.fields.len(), 2);

    // which can also contain extra metadata:
    let mut metadata = Metadata::new();
    metadata.insert(
        "Office Space".to_string(),
        "Deals with real issues in the workplace.".to_string(),
    );
    let schema = schema.with_metadata(metadata);

    assert_eq!(schema.fields.len(), 2);
}
