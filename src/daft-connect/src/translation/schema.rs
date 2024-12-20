use daft_micropartition::partitioning::InMemoryPartitionSetCache;
use spark_connect::{
    data_type::{Kind, Struct, StructField},
    DataType, Relation,
};
use tracing::warn;

use super::SparkAnalyzer;
use crate::translation::to_spark_datatype;

#[tracing::instrument(skip_all)]
pub async fn relation_to_schema(input: Relation) -> eyre::Result<DataType> {
    if let Some(common) = &input.common {
        if common.origin.is_some() {
            warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }
    }

    // We're just checking the schema here, so we don't need to use a persistent cache as it won't be used
    let pset = InMemoryPartitionSetCache::empty();
    let translator = SparkAnalyzer::new(&pset);
    let plan = Box::pin(translator.to_logical_plan(input)).await?;

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
