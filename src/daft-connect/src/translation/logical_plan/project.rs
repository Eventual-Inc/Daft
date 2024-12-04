//! Project operation for selecting and manipulating columns from a dataset
//!
//! TL;DR: Project is Spark's equivalent of SQL SELECT - it selects columns, renames them via aliases,
//! and creates new columns from expressions. Example: `df.select(col("id").alias("my_number"))`

use daft_logical_plan::LogicalPlanBuilder;
use eyre::bail;
use spark_connect::Project;

use crate::translation::{to_daft_expr, to_logical_plan};

pub fn project(project: Project) -> eyre::Result<LogicalPlanBuilder> {
    let Project { input, expressions } = project;

    let Some(input) = input else {
        bail!("Project input is required");
    };

    let plan = to_logical_plan(*input)?;

    let daft_exprs: Vec<_> = expressions.iter().map(to_daft_expr).try_collect()?;

    let plan = plan.select(daft_exprs)?;

    Ok(plan)
}
