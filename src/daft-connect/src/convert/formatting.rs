use spark_connect::relation::RelType;

/// Extension trait for RelType to add a `name` method.
pub trait RelTypeExt {
    /// Returns the name of the RelType as a string.
    fn name(&self) -> &'static str;
}

impl RelTypeExt for RelType {
    fn name(&self) -> &'static str {
        match self {
            Self::Read(_) => "Read",
            Self::Project(_) => "Project",
            Self::Filter(_) => "Filter",
            Self::Join(_) => "Join",
            Self::SetOp(_) => "SetOp",
            Self::Sort(_) => "Sort",
            Self::Limit(_) => "Limit",
            Self::Aggregate(_) => "Aggregate",
            Self::Sql(_) => "Sql",
            Self::LocalRelation(_) => "LocalRelation",
            Self::Sample(_) => "Sample",
            Self::Offset(_) => "Offset",
            Self::Deduplicate(_) => "Deduplicate",
            Self::Range(_) => "Range",
            Self::SubqueryAlias(_) => "SubqueryAlias",
            Self::Repartition(_) => "Repartition",
            Self::ToDf(_) => "ToDf",
            Self::WithColumnsRenamed(_) => "WithColumnsRenamed",
            Self::ShowString(_) => "ShowString",
            Self::Drop(_) => "Drop",
            Self::Tail(_) => "Tail",
            Self::WithColumns(_) => "WithColumns",
            Self::Hint(_) => "Hint",
            Self::Unpivot(_) => "Unpivot",
            Self::ToSchema(_) => "ToSchema",
            Self::RepartitionByExpression(_) => "RepartitionByExpression",
            Self::MapPartitions(_) => "MapPartitions",
            Self::CollectMetrics(_) => "CollectMetrics",
            Self::Parse(_) => "Parse",
            Self::GroupMap(_) => "GroupMap",
            Self::CoGroupMap(_) => "CoGroupMap",
            Self::WithWatermark(_) => "WithWatermark",
            Self::ApplyInPandasWithState(_) => "ApplyInPandasWithState",
            Self::HtmlString(_) => "HtmlString",
            Self::CachedLocalRelation(_) => "CachedLocalRelation",
            Self::CachedRemoteRelation(_) => "CachedRemoteRelation",
            Self::CommonInlineUserDefinedTableFunction(_) => "CommonInlineUserDefinedTableFunction",
            Self::AsOfJoin(_) => "AsOfJoin",
            Self::CommonInlineUserDefinedDataSource(_) => "CommonInlineUserDefinedDataSource",
            Self::WithRelations(_) => "WithRelations",
            Self::Transpose(_) => "Transpose",
            Self::FillNa(_) => "FillNa",
            Self::DropNa(_) => "DropNa",
            Self::Replace(_) => "Replace",
            Self::Summary(_) => "Summary",
            Self::Crosstab(_) => "Crosstab",
            Self::Describe(_) => "Describe",
            Self::Cov(_) => "Cov",
            Self::Corr(_) => "Corr",
            Self::ApproxQuantile(_) => "ApproxQuantile",
            Self::FreqItems(_) => "FreqItems",
            Self::SampleBy(_) => "SampleBy",
            Self::Catalog(_) => "Catalog",
            Self::Extension(_) => "Extension",
            Self::Unknown(_) => "Unknown",
        }
    }
}
