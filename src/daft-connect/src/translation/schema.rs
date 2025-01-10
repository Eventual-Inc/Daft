use daft_schema::schema::SchemaRef;
use spark_connect::{
    data_type::{Kind, Struct, StructField},
    DataType, Relation,
};
use tracing::debug;

use super::SparkAnalyzer;
use crate::translation::to_spark_datatype;

impl SparkAnalyzer<'_> {
    #[tracing::instrument(skip_all)]
    pub async fn relation_to_spark_schema(&self, input: Relation) -> eyre::Result<DataType> {
        let result = self.relation_to_daft_schema(input).await?;

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

    #[tracing::instrument(skip_all)]
    pub async fn relation_to_daft_schema(&self, input: Relation) -> eyre::Result<SchemaRef> {
        if let Some(common) = &input.common {
            if common.origin.is_some() {
                debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
            }
        }

        let plan = Box::pin(self.to_logical_plan(input)).await?;

        let result = plan.schema();

        Ok(result)
    }
}
