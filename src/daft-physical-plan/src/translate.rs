use common_error::DaftResult;
use daft_core::{join::JoinStrategy, prelude::Schema};
use daft_dsl::ExprRef;
use daft_plan::{LogicalPlan, LogicalPlanRef, SourceInfo};

use crate::local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};

pub fn translate(plan: &LogicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => Ok(LocalPhysicalPlan::in_memory_scan(info.clone())),
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks_iter = info.scan_op.0.to_scan_tasks(info.pushdowns.clone())?;
                    let scan_tasks = scan_tasks_iter.collect::<DaftResult<Vec<_>>>()?;
                    if scan_tasks.is_empty() {
                        Ok(LocalPhysicalPlan::empty_scan(source.output_schema.clone()))
                    } else {
                        Ok(LocalPhysicalPlan::physical_scan(
                            scan_tasks,
                            source.output_schema.clone(),
                        ))
                    }
                }
                SourceInfo::PlaceHolder(_) => {
                    panic!("We should not encounter a PlaceHolder during translation")
                }
            }
        }
        LogicalPlan::Filter(filter) => {
            let input = translate(&filter.input)?;
            Ok(LocalPhysicalPlan::filter(input, filter.predicate.clone()))
        }
        LogicalPlan::Limit(limit) => {
            let input = translate(&limit.input)?;
            Ok(LocalPhysicalPlan::limit(input, limit.limit))
        }
        LogicalPlan::Project(project) => {
            let input = translate(&project.input)?;
            Ok(LocalPhysicalPlan::project(
                input,
                project.projection.clone(),
                project.projected_schema.clone(),
            ))
        }
        LogicalPlan::Sample(sample) => {
            let input = translate(&sample.input)?;
            Ok(LocalPhysicalPlan::sample(
                input,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = translate(&aggregate.input)?;
            if aggregate.groupby.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.output_schema.clone(),
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.groupby.clone(),
                    aggregate.output_schema.clone(),
                ))
            }
        }
        LogicalPlan::Unpivot(unpivot) => {
            let input = translate(&unpivot.input)?;
            Ok(LocalPhysicalPlan::unpivot(
                input,
                unpivot.ids.clone(),
                unpivot.values.clone(),
                unpivot.variable_name.clone(),
                unpivot.value_name.clone(),
                unpivot.output_schema.clone(),
            ))
        }
        LogicalPlan::Pivot(pivot) => {
            let input = translate(&pivot.input)?;
            let groupby_with_pivot = pivot
                .group_by
                .iter()
                .chain(std::iter::once(&pivot.pivot_column))
                .cloned()
                .collect::<Vec<_>>();
            let aggregate_fields = groupby_with_pivot
                .iter()
                .map(|expr| expr.to_field(input.schema()))
                .chain(std::iter::once(pivot.aggregation.to_field(input.schema())))
                .collect::<DaftResult<Vec<_>>>()?;
            let aggregate_schema = Schema::new(aggregate_fields)?;
            let aggregate = LocalPhysicalPlan::hash_aggregate(
                input,
                vec![pivot.aggregation.clone(); 1],
                groupby_with_pivot,
                aggregate_schema.into(),
            );
            Ok(LocalPhysicalPlan::pivot(
                aggregate,
                pivot.group_by.clone(),
                pivot.pivot_column.clone(),
                pivot.value_column.clone(),
                pivot.names.clone(),
                pivot.output_schema.clone(),
            ))
        }
        LogicalPlan::Sort(sort) => {
            let input = translate(&sort.input)?;
            Ok(LocalPhysicalPlan::sort(
                input,
                sort.sort_by.clone(),
                sort.descending.clone(),
            ))
        }
        LogicalPlan::Join(join) => {
            if join.join_strategy.is_some_and(|x| x != JoinStrategy::Hash) {
                todo!("Only hash join is supported for now")
            }
            let left = translate(&join.left)?;
            let right = translate(&join.right)?;
            Ok(LocalPhysicalPlan::hash_join(
                left,
                right,
                join.left_on.clone(),
                join.right_on.clone(),
                join.join_type,
                join.output_schema.clone(),
            ))
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let input = translate(&distinct.input)?;
            let col_exprs = input
                .schema()
                .names()
                .iter()
                .map(|name| daft_dsl::col(name.clone()))
                .collect::<Vec<ExprRef>>();
            Ok(LocalPhysicalPlan::hash_aggregate(
                input,
                vec![],
                col_exprs,
                schema,
            ))
        }
        LogicalPlan::Concat(concat) => {
            let input = translate(&concat.input)?;
            let other = translate(&concat.other)?;
            Ok(LocalPhysicalPlan::concat(input, other))
        }
        LogicalPlan::Repartition(repartition) => {
            log::warn!("Repartition Not supported for Local Executor!; This will be a No-Op");
            translate(&repartition.input)
        }
        LogicalPlan::Explode(explode) => {
            let input = translate(&explode.input)?;
            Ok(LocalPhysicalPlan::explode(
                input,
                explode.to_explode.clone(),
                explode.exploded_schema.clone(),
            ))
        }
        _ => todo!("{} not yet implemented", plan.name()),
    }
}
