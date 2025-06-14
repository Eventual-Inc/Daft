use super::{FromToProto, ProtoResult};

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

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        dbg!(message);
        todo!();
        // let rel = match message.rel_variant.unwrap() {
        //     proto::RelVariant::Source(rel_source) => todo!(),
        //     proto::RelVariant::Project(rel_project) => todo!(),
        // };
        // Ok(rel)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let rel_variant = match self {
            //
            //
            ir::rel::LogicalPlan::Source(source) => {
                //
                let schema = source.output_schema.to_proto()?;
                let table = match source.source_info.as_ref() {
                    ir::rel::SourceInfo::InMemory(in_memory_info) => {
                        println!("CANNOT SERIALIIZE A MEMORY SCAN YET!");
                        println!("------------------------------------");
                        dbg!(in_memory_info);
                        "in_memory"
                    }
                    ir::rel::SourceInfo::Physical(physical_scan_info) => {
                        println!("CANNOT SERIALIIZE A PHYSICAL SCAN YET!");
                        println!("--------------------------------------");
                        dbg!(&source);
                        // dbg!(physical_scan_info);
                        "physical"
                    },
                    ir::rel::SourceInfo::PlaceHolder(place_holder_info) => {
                        dbg!(place_holder_info);
                        unreachable!();
                    }
                };
                let source = proto::RelSource {
                    schema: Some(schema),
                    table: table.to_string(),
                };
                proto::rel::RelVariant::Source(source)
            }
            //
            //
            ir::rel::LogicalPlan::Project(project) => {
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
            ir::rel::LogicalPlan::ActorPoolProject(actor_pool_project) => todo!(),
            ir::rel::LogicalPlan::Filter(filter) => todo!(),
            ir::rel::LogicalPlan::Limit(limit) => todo!(),
            ir::rel::LogicalPlan::Explode(explode) => todo!(),
            ir::rel::LogicalPlan::Unpivot(unpivot) => todo!(),
            ir::rel::LogicalPlan::Sort(sort) => todo!(),
            ir::rel::LogicalPlan::Repartition(repartition) => todo!(),
            ir::rel::LogicalPlan::Distinct(distinct) => todo!(),
            ir::rel::LogicalPlan::Aggregate(aggregate) => todo!(),
            ir::rel::LogicalPlan::Pivot(pivot) => todo!(),
            ir::rel::LogicalPlan::Concat(concat) => todo!(),
            ir::rel::LogicalPlan::Intersect(intersect) => todo!(),
            ir::rel::LogicalPlan::Union(union) => todo!(),
            ir::rel::LogicalPlan::Join(join) => todo!(),
            ir::rel::LogicalPlan::Sink(sink) => todo!(),
            ir::rel::LogicalPlan::Sample(sample) => todo!(),
            ir::rel::LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => todo!(),
            ir::rel::LogicalPlan::SubqueryAlias(subquery_alias) => todo!(),
            ir::rel::LogicalPlan::Window(window) => todo!(),
            ir::rel::LogicalPlan::TopN(top_n) => todo!(),
        };
        Ok(Self::Message {
            rel_variant: Some(rel_variant),
        })
    }
}
