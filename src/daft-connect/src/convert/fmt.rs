use std::fmt::Display;

use daft_plan::LogicalPlanRef;
use spark_connect::{relation::RelType, Relation, ShowString};

pub struct TopLevelDisplay<'a> {
    plan: &'a RelType,
}

impl TopLevelDisplay<'_> {
    pub fn new(plan: &RelType) -> Self {
        Self { plan }
    }
}

impl<'a> Display for TopLevelDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rel_type_str = match self {
            RelType::Read(_) => "Read",
            RelType::Project(_) => "Project",
            RelType::Filter(_) => "Filter",
            RelType::Join(_) => "Join",
            RelType::SetOp(_) => "SetOp",
            RelType::Sort(_) => "Sort",
            RelType::Limit(_) => "Limit",
            RelType::Aggregate(_) => "Aggregate",
            RelType::Sql(_) => "Sql",
            RelType::LocalRelation(_) => "LocalRelation",
            RelType::Sample(_) => "Sample",
            RelType::Offset(_) => "Offset",
            RelType::Deduplicate(_) => "Deduplicate",
            RelType::Range(_) => "Range",
            RelType::SubqueryAlias(_) => "SubqueryAlias",
            RelType::Repartition(_) => "Repartition",
            RelType::ToDf(_) => "ToDf",
            RelType::WithColumnsRenamed(_) => "WithColumnsRenamed",
            RelType::ShowString(_) => "ShowString",
            RelType::Drop(_) => "Drop",
            RelType::Tail(_) => "Tail",
            RelType::WithColumns(_) => "WithColumns",
            RelType::Hint(_) => "Hint",
            RelType::Unpivot(_) => "Unpivot",
            RelType::ToSchema(_) => "ToSchema",
            RelType::RepartitionByExpression(_) => "RepartitionByExpression",
            RelType::MapPartitions(_) => "MapPartitions",
            RelType::CollectMetrics(_) => "CollectMetrics",
            RelType::Parse(_) => "Parse",
            RelType::GroupMap(_) => "GroupMap",
            RelType::CoGroupMap(_) => "CoGroupMap",
            RelType::WithWatermark(_) => "WithWatermark",
            RelType::ApplyInPandasWithState(_) => "ApplyInPandasWithState",
            RelType::HtmlString(_) => "HtmlString",
            RelType::CachedLocalRelation(_) => "CachedLocalRelation",
            RelType::CachedRemoteRelation(_) => "CachedRemoteRelation",
            RelType::CommonInlineUserDefinedTableFunction(_) => {
                "CommonInlineUserDefinedTableFunction"
            }
            RelType::AsOfJoin(_) => "AsOfJoin",
            RelType::CommonInlineUserDefinedDataSource(_) => "CommonInlineUserDefinedDataSource",
            RelType::WithRelations(_) => "WithRelations",
            RelType::Transpose(_) => "Transpose",
            RelType::FillNa(_) => "FillNa",
            RelType::DropNa(_) => "DropNa",
            RelType::Replace(_) => "Replace",
            RelType::Summary(_) => "Summary",
            RelType::Crosstab(_) => "Crosstab",
            RelType::Describe(_) => "Describe",
            RelType::Cov(_) => "Cov",
            RelType::Corr(_) => "Corr",
            RelType::ApproxQuantile(_) => "ApproxQuantile",
            RelType::FreqItems(_) => "FreqItems",
            RelType::SampleBy(_) => "SampleBy",
            RelType::Catalog(_) => "Catalog",
            RelType::Extension(_) => "Extension",
            RelType::Unknown(_) => "Unknown",
        };

        f.write_str(rel_type_str)
    }
}
