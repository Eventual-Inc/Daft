use spark_connect::{
    data_type::{Kind, Long, Struct, StructField},
    relation::RelType,
    DataType, Relation,
};

#[tracing::instrument(skip_all)]
pub fn connect_schema(input: Relation) -> Result<DataType, tonic::Status> {
    if input.common.is_some() {
        tracing::warn!("We do not currently look at common fields");
    }

    let result = match input
        .rel_type
        .ok_or_else(|| tonic::Status::internal("rel_type is None"))?
    {
        RelType::Range(spark_connect::Range { num_partitions, .. }) => {
            if num_partitions.is_some() {
                return Err(tonic::Status::unimplemented(
                    "num_partitions is not supported",
                ));
            }

            let long = Long {
                type_variation_reference: 0,
            };

            let id_field = StructField {
                name: "id".to_string(),
                data_type: Some(DataType {
                    kind: Some(Kind::Long(long)),
                }),
                nullable: false,
                metadata: None,
            };

            let fields = vec![id_field];

            let strct = Struct {
                fields,
                type_variation_reference: 0,
            };

            DataType {
                kind: Some(Kind::Struct(strct)),
            }
        }
        other => {
            return Err(tonic::Status::unimplemented(format!(
                "Unsupported relation type: {other:?}"
            )))
        }
    };

    Ok(result)
}
