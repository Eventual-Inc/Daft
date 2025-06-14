use super::{FromToProto, ProtoResult};
use crate::unsupported_err;

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use crate::*;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use daft_proto::protos::daft::v1::*;
}

impl FromToProto for ir::rel::LogicalPlan {
    type Message = proto::Rel;

    fn from_proto(_message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        // let rel = match message.rel_variant.unwrap() {
        //     proto::RelVariant::Source(rel_source) => todo!(),
        //     proto::RelVariant::Project(rel_project) => todo!(),
        // };
        unsupported_err!("LogicalPlan::from_proto");
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let rel_variant = match self {
            Self::Source(source) => {
                //
                let schema = source.output_schema.to_proto()?;
                let table = match source.source_info.as_ref() {
                    ir::rel::SourceInfo::InMemory(_in_memory_info) => "in_memory",
                    ir::rel::SourceInfo::Physical(_physical_scan_info) => "physical",
                    ir::rel::SourceInfo::PlaceHolder(_place_holder_info) => unreachable!(),
                };
                let source = proto::RelSource {
                    schema: Some(schema),
                    table: table.to_string(),
                };
                proto::rel::RelVariant::Source(source)
            }
            Self::Project(project) => {
                let input = project.input.to_proto()?;
                let projections: ProtoResult<Vec<proto::Expr>> = project
                    .projection
                    .iter()
                    .map(|expr| expr.to_proto())
                    .collect();
                let project = proto::RelProject {
                    input: Some(input.into()),
                    projections: projections?,
                };
                proto::rel::RelVariant::Project(project.into())
            }
            Self::ActorPoolProject(_actor_pool_project) => unsupported_err!("actor_pool_project"),
            Self::Filter(_filter) => unsupported_err!("filter"),
            Self::Limit(_limit) => unsupported_err!("limit"),
            Self::Explode(_explode) => unsupported_err!("explode"),
            Self::Unpivot(_unpivot) => unsupported_err!("unpivot"),
            Self::Sort(_sort) => unsupported_err!("sort"),
            Self::Repartition(_repartition) => unsupported_err!("repartition"),
            Self::Distinct(_distinct) => unsupported_err!("distinct"),
            Self::Aggregate(_aggregate) => unsupported_err!("aggregate"),
            Self::Pivot(_pivot) => unsupported_err!("pivot"),
            Self::Concat(_concat) => unsupported_err!("concat"),
            Self::Intersect(_intersect) => unsupported_err!("intersect"),
            Self::Union(_union) => unsupported_err!("union"),
            Self::Join(_join) => unsupported_err!("join"),
            Self::Sink(_sink) => unsupported_err!("sink"),
            Self::Sample(_sample) => unsupported_err!("sample"),
            Self::MonotonicallyIncreasingId(_monotonically_increasing_id) => {
                unsupported_err!("monotonically_increasing_id")
            }
            Self::SubqueryAlias(_subquery_alias) => unsupported_err!("subquery_alias"),
            Self::Window(_window) => unsupported_err!("window"),
            Self::TopN(_top_nn) => unsupported_err!("top_n"),
        };
        Ok(Self::Message {
            rel_variant: Some(rel_variant),
        })
    }
}
