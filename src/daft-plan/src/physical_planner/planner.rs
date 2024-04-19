use std::cmp::Ordering;
use std::sync::Arc;
use std::{
    cmp::{max, min},
    collections::HashMap,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeVisitor};
use daft_core::count_mode::CountMode;
use daft_core::DataType;
use daft_dsl::Expr;
use daft_scan::ScanExternalInfo;

use crate::logical_ops::{
    Aggregate as LogicalAggregate, Distinct as LogicalDistinct, Explode as LogicalExplode,
    Filter as LogicalFilter, Join as LogicalJoin, Limit as LogicalLimit,
    MonotonicallyIncreasingId as LogicalMonotonicallyIncreasingId, Project as LogicalProject,
    Repartition as LogicalRepartition, Sample as LogicalSample, Sink as LogicalSink,
    Sort as LogicalSort, Source,
};
use crate::logical_plan::LogicalPlan;
use crate::partitioning::{
    ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
};
use crate::physical_plan::PhysicalPlan;
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::SourceInfo;
use crate::FileFormat;
use crate::{physical_ops::*, JoinStrategy};

#[cfg(feature = "python")]
use crate::physical_ops::InMemoryScan;

use common_treenode::VisitRecursion;

use super::translate::translate_single_logical_node;
pub(super) struct PhysicalPlanTranslator {
    pub physical_children: Vec<PhysicalPlan>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeVisitor for PhysicalPlanTranslator {
    type N = LogicalPlan;
    fn pre_visit(&mut self, _node: &Self::N) -> DaftResult<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(&mut self, node: &Self::N) -> DaftResult<VisitRecursion> {
        let output = translate_single_logical_node(node, &mut self.physical_children, &self.cfg)?;
        self.physical_children.push(output);
        Ok(VisitRecursion::Continue)
    }
}
