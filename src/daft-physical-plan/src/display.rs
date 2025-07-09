use std::fmt::{self, Display};

use common_display::{tree::TreeDisplay, DisplayLevel};

use super::PhysicalPlan;

impl TreeDisplay for PhysicalPlan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match self {
            Self::InMemoryScan(scan) => scan.display_as(level),
            Self::TabularScan(scan) => scan.display_as(level),
            Self::EmptyScan(scan) => scan.display_as(level),
            Self::PreviousStageScan(scan) => scan.display_as(level),
            Self::Project(p) => p.display_as(level),
            Self::ActorPoolProject(p) => p.display_as(level),
            Self::Filter(f) => f.display_as(level),
            Self::Limit(limit) => limit.display_as(level),
            Self::Explode(explode) => explode.display_as(level),
            Self::Unpivot(unpivot) => unpivot.display_as(level),
            Self::Sort(sort) => sort.display_as(level),
            Self::TopN(top_n) => top_n.display_as(level),
            Self::Sample(sample) => sample.display_as(level),
            Self::MonotonicallyIncreasingId(id) => id.display_as(level),
            Self::ShuffleExchange(shuffle_exchange) => shuffle_exchange.display_as(level),
            Self::Aggregate(aggr) => aggr.display_as(level),
            Self::Dedup(dedup) => dedup.display_as(level),
            Self::Pivot(pivot) => pivot.display_as(level),
            Self::Concat(concat) => concat.display_as(level),
            Self::HashJoin(join) => join.display_as(level),
            Self::SortMergeJoin(join) => join.display_as(level),
            Self::BroadcastJoin(join) => join.display_as(level),
            Self::CrossJoin(join) => join.display_as(level),
            Self::TabularWriteParquet(write) => write.display_as(level),
            Self::TabularWriteJson(write) => write.display_as(level),
            Self::TabularWriteCsv(write) => write.display_as(level),
            #[cfg(feature = "python")]
            Self::IcebergWrite(write) => write.display_as(level),
            #[cfg(feature = "python")]
            Self::DeltaLakeWrite(write) => write.display_as(level),
            #[cfg(feature = "python")]
            Self::LanceWrite(write) => write.display_as(level),
            #[cfg(feature = "python")]
            Self::DataSink(write) => write.display_as(level),
        }
    }

    fn get_name(&self) -> String {
        self.name()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children().into_iter().map(|x| x as _).collect()
    }
}

// Single node display.
impl Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.multiline_display().join(", "))?;
        Ok(())
    }
}
