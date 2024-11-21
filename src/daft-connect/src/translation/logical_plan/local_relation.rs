use daft_logical_plan::LogicalPlanBuilder;

pub fn local_relation(
    local_relation: spark_connect::LocalRelation,
) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::LocalRelation {
        data,
        schema,
    } = local_relation;
}
