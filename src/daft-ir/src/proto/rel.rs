use std::sync::Arc;

use daft_logical_plan::ops::ActorPoolProject;

use super::{ProtoResult, ToFromProto};
use crate::{
    non_null, not_implemented_err, not_optimized_err,
    proto::{from_proto, from_proto_arc, from_protos, to_proto_vec, to_protos, UNIT},
};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use crate::*;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use daft_proto::protos::daft::v1::*;
    pub use daft_proto::protos::daft::v1::rel::Variant as RelVariant;
    pub use daft_proto::protos::daft::v1::source_info::Variant as SourceInfoVariant;
    pub use daft_proto::protos::daft::v1::partition_transform::Variant as PartitionTransformVariant;
}

#[allow(unused)]
impl ToFromProto for ir::rel::LogicalPlan {
    type Message = proto::Rel;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let plan = match message.variant.unwrap() {
            proto::RelVariant::Source(source) => {
                let source = ir::rel::Source::from_proto(*source)?;
                Self::Source(source)
            }
            proto::RelVariant::Project(project) => {
                let project = ir::rel::Project::from_proto(*project)?;
                Self::Project(project)
            }
            proto::RelVariant::ActorPoolProject(actor_pool_project) => {
                let actor_pool_project =
                    ir::rel::ActorPoolProject::from_proto(*actor_pool_project)?;
                Self::ActorPoolProject(actor_pool_project)
            }
            proto::RelVariant::Filter(filter) => {
                let filter = ir::rel::Filter::from_proto(*filter)?;
                Self::Filter(filter)
            }
            proto::RelVariant::Limit(limit) => {
                let limit = ir::rel::Limit::from_proto(*limit)?;
                Self::Limit(limit)
            }
            proto::RelVariant::Explode(explode) => {
                not_implemented_err!("explode");
                // let explode = ir::rel::Explode::from_proto(*explode)?;
                // Self::Explode(explode)
            }
            proto::RelVariant::Unpivot(unpivot) => {
                not_implemented_err!("unpivot");
                // let unpivot = ir::rel::Unpivot::from_proto(*unpivot)?;
                // Self::Unpivot(unpivot)
            }
            proto::RelVariant::Sort(sort) => {
                not_implemented_err!("sort");
                // let sort = ir::rel::Sort::from_proto(*sort)?;
                // Self::Sort(sort)
            }
            proto::RelVariant::Repartition(repartition) => {
                not_implemented_err!("repartition");
                // let repartition = ir::rel::Repartition::from_proto(*repartition)?;
                // Self::Repartition(repartition)
            }
            proto::RelVariant::Distinct(distinct) => {
                let distinct = ir::rel::Distinct::from_proto(*distinct)?;
                Self::Distinct(distinct)
            }
            proto::RelVariant::Aggregate(aggregate) => {
                let aggregate = ir::rel::Aggregate::from_proto(*aggregate)?;
                Self::Aggregate(aggregate)
            }
            proto::RelVariant::Pivot(pivot) => {
                not_implemented_err!("pivot");
                // let pivot = ir::rel::Pivot::from_proto(*pivot)?;
                // Self::Pivot(pivot)
            }
            proto::RelVariant::Concat(concat) => {
                let concat = ir::rel::Concat::from_proto(*concat)?;
                Self::Concat(concat)
            }
            proto::RelVariant::Union(union_) => {
                let union_ = ir::rel::Union::from_proto(*union_)?;
                Self::Union(union_)
            }
            proto::RelVariant::Except(_) => {
                // note: Except is current lowered upon creation. There is a TODO on
                //       the factory method to move the lowering to optimization. At
                //       present, the domain split isn't clear, so I'll leave this
                //       placeholder which should be unreachable.
                not_optimized_err!("except should be removed by optimization.")
            }
            proto::RelVariant::Intersect(intersect) => {
                let intersect = ir::rel::Intersect::from_proto(*intersect)?;
                Self::Intersect(intersect)
            }
            proto::RelVariant::Join(join) => {
                not_implemented_err!("join");
                // let join = ir::rel::Join::from_proto(*join)?;
                // Self::Join(join)
            }
            proto::RelVariant::Sink(sink) => {
                not_implemented_err!("sink");
                // let sink = ir::rel::Sink::from_proto(*sink)?;
                // Self::Sink(sink)
            }
            proto::RelVariant::Sample(sample) => {
                not_implemented_err!("sample");
                // let sample = ir::rel::Sample::from_proto(*sample)?;
                // Self::Sample(sample)
            }
            proto::RelVariant::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                not_implemented_err!("monotonically_increasing_id");
                // let monotonically_increasing_id = ir::rel::MonotonicallyIncreasingId::from_proto(*monotonically_increasing_id)?;
                // Self::MonotonicallyIncreasingId(monotonically_increasing_id)
            }
            proto::RelVariant::SubqueryAlias(subquery_alias) => {
                not_implemented_err!("subquery_alias");
                // let subquery_alias = ir::rel::SubqueryAlias::from_proto(*subquery_alias)?;
                // Self::SubqueryAlias(subquery_alias)
            }
            proto::RelVariant::Window(window) => {
                not_implemented_err!("window");
                // let window = ir::rel::Window::from_proto(*window)?;
                // Self::Window(window)
            }
            proto::RelVariant::TopN(top_n) => {
                not_implemented_err!("top_n");
                // let top_n = ir::rel::TopN::from_proto(*top_n)?;
                // Self::TopN(top_n)
            }
        };
        Ok(plan)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Source(source) => {
                let source = source.to_proto()?.into();
                proto::RelVariant::Source(source)
            }
            Self::Shard(_) => {
                not_implemented_err!("shard");
            }
            Self::Project(project) => {
                let project = project.to_proto()?.into();
                proto::RelVariant::Project(project)
            }
            Self::ActorPoolProject(actor_pool_project) => {
                let actor_pool_project = actor_pool_project.to_proto()?.into();
                proto::RelVariant::ActorPoolProject(actor_pool_project)
            }
            Self::Filter(filter) => {
                let filter = filter.to_proto()?.into();
                proto::RelVariant::Filter(filter)
            }
            Self::Limit(limit) => {
                let limit = limit.to_proto()?.into();
                proto::RelVariant::Limit(limit)
            }
            Self::Explode(explode) => {
                not_implemented_err!("explode");
                // let explode = explode.to_proto()?.into();
                // proto::RelVariant::Explode(explode)
            }
            Self::Unpivot(unpivot) => {
                not_implemented_err!("unpivot");
                // let unpivot = unpivot.to_proto()?.into();
                // proto::RelVariant::Unpivot(unpivot)
            }
            Self::Sort(sort) => {
                not_implemented_err!("sort");
                // let sort = sort.to_proto()?.into();
                // proto::RelVariant::Sort(sort)
            }
            Self::Repartition(repartition) => {
                not_implemented_err!("repartition");
                // let repartition = repartition.to_proto()?.into();
                // proto::RelVariant::Repartition(repartition)
            }
            Self::Distinct(distinct) => {
                let distinct = distinct.to_proto()?.into();
                proto::RelVariant::Distinct(distinct)
            }
            Self::Aggregate(aggregate) => {
                let aggregate = aggregate.to_proto()?.into();
                proto::RelVariant::Aggregate(aggregate)
            }
            Self::Pivot(pivot) => {
                not_implemented_err!("pivot");
                // let pivot = pivot.to_proto()?.into();
                // proto::RelVariant::Pivot(pivot)
            }
            Self::Concat(concat) => {
                let concat = concat.to_proto()?.into();
                proto::RelVariant::Concat(concat)
            }
            Self::Intersect(intersect) => {
                let intersect = intersect.to_proto()?.into();
                proto::RelVariant::Intersect(intersect)
            }
            Self::Union(union_) => {
                let union_ = union_.to_proto()?.into();
                proto::RelVariant::Union(union_)
            }
            Self::Join(join) => {
                not_implemented_err!("join");
                // let join = join.to_proto()?.into();
                // proto::RelVariant::Join(join)
            }
            Self::Sink(sink) => {
                not_implemented_err!("sink");
                // let sink = sink.to_proto()?.into();
                // proto::RelVariant::Sink(sink)
            }
            Self::Sample(sample) => {
                not_implemented_err!("sample");
                // let sample = sample.to_proto()?.into();
                // proto::RelVariant::Sample(sample)
            }
            Self::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                not_implemented_err!("monotonically_increasing_id");
                // let monotonically_increasing_id = monotonically_increasing_id.to_proto()?.into();
                // proto::RelVariant::MonotonicallyIncreasingId(monotonically_increasing_id)
            }
            Self::SubqueryAlias(subquery_alias) => {
                not_implemented_err!("subquery_alias");
                // let subquery_alias = subquery_alias.to_proto()?.into();
                // proto::RelVariant::SubqueryAlias(subquery_alias)
            }
            Self::Window(window) => {
                not_implemented_err!("window");
                // let window = window.to_proto()?.into();
                // proto::RelVariant::Window(window)
            }
            Self::TopN(top_n) => {
                not_implemented_err!("top_n");
                // let top_n = top_n.to_proto()?.into();
                // proto::RelVariant::TopN(top_n)
            }
        };
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

impl ToFromProto for ir::rel::Source {
    type Message = proto::RelSource;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let schema = ir::Schema::from_proto(non_null!(message.schema))?;
        let info = ir::rel::SourceInfo::from_proto(*non_null!(message.info))?;
        Ok(ir::rel::new_source(schema, info)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let schema = self.output_schema.to_proto()?;
        let info = self.source_info.to_proto()?.into();
        Ok(proto::RelSource {
            schema: Some(schema),
            info: Some(info),
        })
    }
}

impl ToFromProto for ir::rel::Project {
    type Message = proto::RelProject;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = ir::rel::LogicalPlan::from_proto(*non_null!(message.input))?;
        let projections = from_protos(message.projections)?;
        Ok(ir::rel::new_project(input, projections)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        let projections = to_protos(&self.projection)?;
        Ok(proto::RelProject {
            input: Some(input),
            projections,
        })
    }
}

impl ToFromProto for ir::rel::Filter {
    type Message = proto::RelFilter;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = from_proto_arc(message.input)?;
        let predicate = from_proto_arc(message.predicate)?;
        Ok(ir::rel::new_filter(input, predicate)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        let predicate = self.predicate.to_proto()?.into();
        Ok(Self::Message {
            input: Some(input),
            predicate: Some(predicate),
        })
    }
}

impl ToFromProto for ir::rel::Limit {
    type Message = proto::RelLimit;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = from_proto_arc(message.input)?;
        let limit = message.limit;
        Ok(ir::rel::new_limit(input, limit)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        let limit = self.limit;
        Ok(Self::Message {
            input: Some(input),
            limit,
        })
    }
}

impl ToFromProto for ir::rel::Distinct {
    type Message = proto::RelDistinct;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = from_proto_arc(message.input)?;
        Ok(ir::rel::new_distinct(input)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        Ok(Self::Message { input: Some(input) })
    }
}

impl ToFromProto for ir::rel::Concat {
    type Message = proto::RelConcat;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let lhs = from_proto_arc(message.lhs)?;
        let rhs = from_proto_arc(message.rhs)?;
        Ok(ir::rel::new_concat(lhs, rhs)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let lhs = self.input.to_proto()?.into();
        let rhs = self.other.to_proto()?.into();
        Ok(Self::Message {
            lhs: Some(lhs),
            rhs: Some(rhs),
        })
    }
}

impl ToFromProto for ir::rel::Intersect {
    type Message = proto::RelIntersect;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let lhs = from_proto_arc(message.lhs)?;
        let rhs = from_proto_arc(message.rhs)?;
        let is_all = message.is_all;
        Ok(ir::rel::new_intersect(lhs, rhs, is_all)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let lhs = self.lhs.to_proto()?.into();
        let rhs = self.rhs.to_proto()?.into();
        let is_all = self.is_all;
        Ok(Self::Message {
            lhs: Some(lhs),
            rhs: Some(rhs),
            is_all,
        })
    }
}

impl ToFromProto for ir::rel::Union {
    type Message = proto::RelUnion;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let lhs = from_proto_arc(message.lhs)?;
        let rhs = from_proto_arc(message.rhs)?;
        let is_all = message.is_all;
        let is_by_name = message.is_by_name;
        Ok(ir::rel::new_union(lhs, rhs, is_all, is_by_name)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let lhs = self.lhs.to_proto()?.into();
        let rhs = self.rhs.to_proto()?.into();
        let is_all = matches!(self.quantifier, ir::rel::SetQuantifier::All);
        let is_by_name = matches!(self.strategy, ir::rel::UnionStrategy::ByName);
        Ok(Self::Message {
            lhs: Some(lhs),
            rhs: Some(rhs),
            is_all,
            is_by_name,
        })
    }
}

impl ToFromProto for ir::rel::Except {
    type Message = proto::RelExcept;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let lhs = from_proto_arc(message.lhs)?;
        let rhs = from_proto_arc(message.rhs)?;
        let is_all = message.is_all;
        Ok(ir::rel::new_except(lhs, rhs, is_all)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let lhs = self.lhs.to_proto()?.into();
        let rhs = self.rhs.to_proto()?.into();
        let is_all = self.is_all;
        Ok(Self::Message {
            lhs: Some(lhs),
            rhs: Some(rhs),
            is_all,
        })
    }
}

impl ToFromProto for ir::rel::Aggregate {
    type Message = proto::RelAggregate;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = from_proto_arc(message.input)?;
        let groups = from_protos(message.groups)?;

        // turn the aggs into expressions .. don't forget to handle the alias!
        let mut aggs = vec![];
        for measure in message.measures {
            let agg = from_proto(measure.agg)?;
            let expr = ir::Expr::Agg(agg);
            let expr = match measure.alias {
                Some(alias) => ir::Expr::Alias(expr.into(), alias.into()),
                None => expr,
            };
            aggs.push(expr);
        }

        Ok(ir::rel::new_aggregate(input, aggs, groups)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        let groups = to_proto_vec(self.groupby.iter())?;

        // Convert measures, supports both `count(arg)` and `count(arg) as alias`.
        let mut measures = vec![];
        for expr in &self.aggregations {
            let measure = match expr.as_ref() {
                ir::Expr::Agg(agg) => {
                    let agg = agg.to_proto()?;
                    proto::Measure {
                        agg: Some(agg),
                        alias: None,
                    }
                }
                ir::Expr::Alias(agg, alias) => {
                    // We should seriously consider removing binding names from aggregations.
                    // This does not need to be recursive. This problem stems from the fact
                    // that we model aggregations as an Expr in the logical IR, when it should
                    // be its own domain.
                    let agg = match agg.as_ref() {
                        ir::Expr::Agg(agg) => agg.to_proto()?,
                        _ => not_optimized_err!(
                            "encountered a scalar expression in an aggregation: {}",
                            expr
                        ),
                    };
                    let alias = alias.to_string();
                    proto::Measure {
                        agg: Some(agg),
                        alias: Some(alias),
                    }
                }
                _ => not_optimized_err!(
                    "encountered a scalar expression in an aggregation: {}",
                    expr
                ),
            };
            measures.push(measure);
        }
        Ok(Self::Message {
            input: Some(input),
            measures,
            groups,
        })
    }
}

impl ToFromProto for ActorPoolProject {
    type Message = proto::RelActorPoolProject;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = ir::rel::LogicalPlan::from_proto(*non_null!(message.input))?;
        let projections = from_protos(message.projections)?;
        Ok(ir::rel::new_project_with_actor_pool(input, projections)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?.into();
        let projections = to_protos(&self.projection)?;
        Ok(proto::RelActorPoolProject {
            input: Some(input),
            projections,
        })
    }
}

// -----------------------------------------------------------------
//
//                            OTHER
//
// -----------------------------------------------------------------

impl ToFromProto for ir::rel::SourceInfo {
    type Message = proto::SourceInfo;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let info = match message.variant.unwrap() {
            proto::SourceInfoVariant::CacheInfo(info) => {
                let info = ir::rel::InMemoryInfo::from_proto(info)?;
                Self::InMemory(info)
            }
            proto::SourceInfoVariant::ScanInfo(info) => {
                let info = ir::rel::PhysicalScanInfo::from_proto(*info)?;
                Self::Physical(info)
            }
        };
        Ok(info)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::InMemory(info) => {
                let info = info.to_proto()?;
                proto::SourceInfoVariant::CacheInfo(info)
            }
            Self::Physical(info) => {
                let info = info.to_proto()?.into();
                proto::SourceInfoVariant::ScanInfo(info)
            }
            Self::PlaceHolder(_) => {
                // err!
                not_optimized_err!(
                    "source_info placeholder should have been removed by optimizer."
                );
            }
        };
        Ok(proto::SourceInfo {
            variant: Some(variant),
        })
    }
}

impl ToFromProto for ir::rel::InMemoryInfo {
    type Message = proto::source_info::CacheInfo;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let schema = ir::Schema::from_proto(non_null!(message.schema))?;
        Ok(Self::new(
            schema.into(),
            message.cache_key,
            None, // cache_entry is not serialized in proto
            message.num_partitions as usize,
            message.size_bytes as usize,
            message.num_rows as usize,
            None, // clustering_spec is not serialized in proto
            None, // source_stage_id is not serialized in proto
        ))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        Ok(proto::source_info::CacheInfo {
            schema: Some(self.source_schema.to_proto()?),
            cache_key: self.cache_key.clone(),
            num_partitions: self.num_partitions as u64,
            num_rows: self.num_rows as u64,
            size_bytes: self.size_bytes as u64,
        })
    }
}

impl ToFromProto for ir::rel::PhysicalScanInfo {
    type Message = proto::source_info::ScanInfo;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let schema = ir::Schema::from_proto(non_null!(message.schema))?;
        let partitioning_keys = message
            .partitions
            .map(|p| {
                p.partitions
                    .into_iter()
                    .map(ir::PartitionField::from_proto)
                    .collect::<ProtoResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let pushdowns = ir::Pushdowns::default(); // pushdowns are no serialized yet

        //
        let scan_state = if let Some(tasks) = message.tasks {
            let tasks = tasks
                .tasks
                .into_iter()
                .map(ir::ScanTaskLikeRef::from_proto)
                .collect::<ProtoResult<Vec<_>>>()?;
            ir::ScanState::Tasks(Arc::new(tasks))
        } else {
            not_optimized_err!("missing tasks in PhysicalScanInfo")
        };

        Ok(Self {
            scan_state,
            source_schema: schema.into(),
            partitioning_keys,
            pushdowns,
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        //  convert tasks first to ensure the plan is optimized.
        let tasks = match &self.scan_state {
            ir::ScanState::Tasks(tasks) => {
                let tasks = to_proto_vec(tasks.as_ref())?;
                proto::ScanTasks { tasks }
            }
            ir::ScanState::Operator(_) => {
                // err!
                not_optimized_err!("unexpected ScanOperator in optimized plans.")
            }
        };
        // convert schema
        let schema = self.source_schema.to_proto()?;
        // convert partition fields
        let partitions = to_proto_vec(&self.partitioning_keys)?;
        let partitions = proto::PartitionFields { partitions };
        // convert pushdowns
        // todo(conner)
        //
        Ok(proto::source_info::ScanInfo {
            schema: Some(schema),
            partitions: Some(partitions),
            pushdowns: None,
            tasks: Some(tasks),
        })
    }
}

impl ToFromProto for ir::PartitionField {
    type Message = proto::PartitionField;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let field = ir::schema::Field::from_proto(non_null!(message.field))?;
        let source = message
            .source
            .map(ir::schema::Field::from_proto)
            .transpose()?;
        let transform = message
            .transform
            .map(ir::PartitionTransform::from_proto)
            .transpose()?;
        Ok(Self {
            field,
            source_field: source,
            transform,
        })
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let field = self.field.to_proto()?;
        let source = self
            .source_field
            .as_ref()
            .map(|s| s.to_proto())
            .transpose()?;
        let transform = self.transform.as_ref().map(|t| t.to_proto()).transpose()?;
        Ok(proto::PartitionField {
            field: Some(field),
            source,
            transform,
        })
    }
}

impl ToFromProto for ir::PartitionTransform {
    type Message = proto::PartitionTransform;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let variant = match message.variant.unwrap() {
            proto::PartitionTransformVariant::Identity(_) => Self::Identity,
            proto::PartitionTransformVariant::Year(_) => Self::Year,
            proto::PartitionTransformVariant::Month(_) => Self::Month,
            proto::PartitionTransformVariant::Day(_) => Self::Day,
            proto::PartitionTransformVariant::Hour(_) => Self::Hour,
            proto::PartitionTransformVariant::Void(_) => Self::Void,
            proto::PartitionTransformVariant::IcebergBucket(b) => Self::IcebergBucket(b.buckets),
            proto::PartitionTransformVariant::IcebergTruncate(t) => Self::IcebergTruncate(t.width),
        };
        Ok(variant)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Identity => proto::PartitionTransformVariant::Identity(UNIT),
            Self::Year => proto::PartitionTransformVariant::Year(UNIT),
            Self::Month => proto::PartitionTransformVariant::Month(UNIT),
            Self::Day => proto::PartitionTransformVariant::Day(UNIT),
            Self::Hour => proto::PartitionTransformVariant::Hour(UNIT),
            Self::Void => proto::PartitionTransformVariant::Void(UNIT),
            Self::IcebergBucket(buckets) => proto::PartitionTransformVariant::IcebergBucket(
                proto::partition_transform::IcebergBucket { buckets: *buckets },
            ),
            Self::IcebergTruncate(width) => proto::PartitionTransformVariant::IcebergTruncate(
                proto::partition_transform::IcebergTruncate { width: *width },
            ),
        };
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

// This was too intimidating, but we can shove the bytes into a wrapper atm.
impl ToFromProto for ir::ScanTaskLikeRef {
    type Message = proto::ScanTask;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        Ok(bincode::deserialize(&message.task)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        Ok(Self::Message {
            task: bincode::serialize(self)?,
        })
    }
}
