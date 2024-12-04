use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, WrapErr};
use spark_connect::aggregate::GroupType;

use crate::translation::{to_daft_expr, to_logical_plan};

pub fn aggregate(aggregate: spark_connect::Aggregate) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::Aggregate {
        input,
        group_type,
        grouping_expressions,
        aggregate_expressions,
        pivot,
        grouping_sets,
    } = aggregate;

    let Some(input) = input else {
        bail!("input is required");
    };

    let plan = to_logical_plan(*input)?;

    let group_type = GroupType::try_from(group_type)
        .wrap_err_with(|| format!("Invalid group type: {group_type:?}"))?;

    assert_groupby(group_type)?;

    if let Some(pivot) = pivot {
        bail!("Pivot not yet supported; got {pivot:?}");
    }

    if !grouping_sets.is_empty() {
        bail!("Grouping sets not yet supported; got {grouping_sets:?}");
    }

    let grouping_expressions: Vec<_> = grouping_expressions
        .iter()
        .map(to_daft_expr)
        .try_collect()?;

    let aggregate_expressions: Vec<_> = aggregate_expressions
        .iter()
        .map(to_daft_expr)
        .try_collect()?;

    let plan = plan
                .aggregate(aggregate_expressions.clone(), grouping_expressions.clone())
                .wrap_err_with(|| format!("Failed to apply aggregate to logical plan aggregate_expressions={aggregate_expressions:?} grouping_expressions={grouping_expressions:?}"))?;

    Ok(plan)
}

fn assert_groupby(plan: GroupType) -> eyre::Result<()> {
    match plan {
        GroupType::Unspecified => {
            bail!("GroupType must be specified; got Unspecified")
        }
        GroupType::Groupby => Ok(()),
        GroupType::Rollup => {
            bail!("Rollup not yet supported")
        }
        GroupType::Cube => {
            bail!("Cube not yet supported")
        }
        GroupType::Pivot => {
            bail!("Pivot not yet supported")
        }
        GroupType::GroupingSets => {
            bail!("GroupingSets not yet supported")
        }
    }
}
