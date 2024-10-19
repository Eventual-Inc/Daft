use std::{collections::HashSet, sync::Arc};

use daft_plan::{LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, ParquetScanBuilder};
use spark_connect::{
    expression::Alias,
    read::{DataSource, ReadType},
    relation::RelType,
    Filter, Read, Relation, ShowString, WithColumns,
};

mod expr;

mod logical_plan;

mod stream;

mod fmt;
