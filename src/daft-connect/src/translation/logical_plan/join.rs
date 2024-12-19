use eyre::{bail, WrapErr};
use spark_connect::join::JoinType;
use tracing::warn;

use crate::translation::{to_logical_plan, Plan};

pub async fn join(join: spark_connect::Join) -> eyre::Result<Plan> {
    let spark_connect::Join {
        left,
        right,
        join_condition,
        join_type,
        using_columns,
        join_data_type,
    } = join;

    let Some(left) = left else {
        bail!("Left side of join is required");
    };

    let Some(right) = right else {
        bail!("Right side of join is required");
    };

    if let Some(join_condition) = join_condition {
        bail!("Join conditions are not yet supported; use using_columns (join keys) instead; got {join_condition:?}");
    }

    let join_type = JoinType::try_from(join_type)
        .wrap_err_with(|| format!("Invalid join type: {join_type:?}"))?;

    let join_type = to_daft_join_type(join_type)?;

    let using_columns_exprs: Vec<_> = using_columns
        .iter()
        .map(|s| daft_dsl::col(s.as_str()))
        .collect();

    if let Some(join_data_type) = join_data_type {
        warn!("Ignoring join data type {join_data_type:?} for join; not yet implemented");
    }

    let mut left = Box::pin(to_logical_plan(*left)).await?;
    let right = Box::pin(to_logical_plan(*right)).await?;

    left.psets.partitions.extend(right.psets.partitions);

    let builder = match join_type {
        JoinTypeInfo::Cross => {
            left.builder.cross_join(&right.builder, None, None)? // todo(correctness): is this correct?
        }
        JoinTypeInfo::Regular(join_type) => {
            left.builder.join(
                &right.builder,
                // join_conditions.clone(), // todo(correctness): is this correct?
                // join_conditions,         // todo(correctness): is this correct?
                using_columns_exprs.clone(),
                using_columns_exprs,
                join_type,
                None,
                None,
                None,
                false, // todo(correctness): we want join keys or not
            )?
        }
    };

    let result = Plan {
        builder,
        psets: left.psets,
    };

    Ok(result)
}

enum JoinTypeInfo {
    Regular(daft_core::join::JoinType),
    Cross,
}

impl From<daft_logical_plan::JoinType> for JoinTypeInfo {
    fn from(join_type: daft_logical_plan::JoinType) -> Self {
        Self::Regular(join_type)
    }
}

fn to_daft_join_type(join_type: JoinType) -> eyre::Result<JoinTypeInfo> {
    match join_type {
        JoinType::Unspecified => {
            bail!("Join type must be specified; got Unspecified")
        }
        JoinType::Inner => Ok(daft_core::join::JoinType::Inner.into()),
        JoinType::FullOuter => {
            bail!("Full outer joins not yet supported") // todo(completeness): add support for full outer joins if it is not already implemented
        }
        JoinType::LeftOuter => Ok(daft_core::join::JoinType::Left.into()), // todo(correctness): is this correct?
        JoinType::RightOuter => Ok(daft_core::join::JoinType::Right.into()),
        JoinType::LeftAnti => Ok(daft_core::join::JoinType::Anti.into()), // todo(correctness): is this correct?
        JoinType::LeftSemi => bail!("Left semi joins not yet supported"), // todo(completeness): add support for left semi joins if it is not already implemented
        JoinType::Cross => Ok(JoinTypeInfo::Cross),
    }
}
