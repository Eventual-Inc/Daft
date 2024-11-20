//! Project operation for selecting and manipulating columns from a dataset
//!
//! TL;DR: Project is Spark's equivalent of SQL SELECT - it selects columns, renames them via aliases,
//! and creates new columns from expressions. Example: `df.select(col("id").alias("my_number"))`

use eyre::bail;
use spark_connect::Project;

use crate::translation::{logical_plan::Plan, to_daft_expr, to_logical_plan};

pub fn project(project: Project) -> eyre::Result<Plan> {
    let Project { input, expressions } = project;

    let Some(input) = input else {
        bail!("Project input is required");
    };

    let mut plan = to_logical_plan(*input)?;

    let daft_exprs: Vec<_> = expressions.iter().map(to_daft_expr).try_collect()?;
    plan.builder = plan.builder.select(daft_exprs)?;

    Ok(plan)
}
