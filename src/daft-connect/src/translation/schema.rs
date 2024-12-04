use spark_connect::{
    data_type::{Kind, Struct, StructField},
    DataType, Relation,
};
use tracing::warn;

use crate::translation::{to_logical_plan, to_spark_datatype};

#[tracing::instrument(skip_all)]
pub fn relation_to_schema(input: Relation) -> eyre::Result<DataType> {
    if input.common.is_some() {
        warn!("We do not currently look at common fields");
    }

    let plan = to_logical_plan(input)?;

    let result = plan.schema();

    let fields: eyre::Result<Vec<StructField>> = result
        .fields
        .iter()
        .map(|(name, field)| {
            let field_type = to_spark_datatype(&field.dtype);
            Ok(StructField {
                name: name.clone(), // todo(correctness): name vs field.name... will they always be the same?
                data_type: Some(field_type),
                nullable: true, // todo(correctness): is this correct?
                metadata: None, // todo(completeness): might want to add metadata here
            })
        })
        .collect();

    Ok(DataType {
        kind: Some(Kind::Struct(Struct {
            fields: fields?,
            type_variation_reference: 0,
        })),
    })
}
