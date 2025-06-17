use std::sync::Arc;

use super::{ProtoResult, ToFromProto};
use crate::{
    non_null, not_implemented_err, not_optimized_err, proto::{from_protos, to_proto_vec, to_protos, UNIT}
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
        };
        Ok(plan)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Source(source) => {
                let source = source.to_proto()?.into();
                proto::RelVariant::Source(source)
            }
            Self::Project(project) => {
                let project = project.to_proto()?.into();
                proto::RelVariant::Project(project)
            }
            Self::ActorPoolProject(_actor_pool_project) => {
                not_implemented_err!("actor_pool_project")
            }
            Self::Filter(_filter) => not_implemented_err!("filter"),
            Self::Limit(_limit) => not_implemented_err!("limit"),
            Self::Explode(_explode) => not_implemented_err!("explode"),
            Self::Unpivot(_unpivot) => not_implemented_err!("unpivot"),
            Self::Sort(_sort) => not_implemented_err!("sort"),
            Self::Repartition(_repartition) => not_implemented_err!("repartition"),
            Self::Distinct(_distinct) => not_implemented_err!("distinct"),
            Self::Aggregate(_aggregate) => not_implemented_err!("aggregate"),
            Self::Pivot(_pivot) => not_implemented_err!("pivot"),
            Self::Concat(_concat) => not_implemented_err!("concat"),
            Self::Intersect(_intersect) => not_implemented_err!("intersect"),
            Self::Union(_union) => not_implemented_err!("union"),
            Self::Join(_join) => not_implemented_err!("join"),
            Self::Sink(_sink) => not_implemented_err!("sink"),
            Self::Sample(_sample) => not_implemented_err!("sample"),
            Self::MonotonicallyIncreasingId(_monotonically_increasing_id) => {
                not_implemented_err!("monotonically_increasing_id")
            }
            Self::SubqueryAlias(_subquery_alias) => not_implemented_err!("subquery_alias"),
            Self::Window(_window) => not_implemented_err!("window"),
            Self::TopN(_top_nn) => not_implemented_err!("top_n"),
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
        Ok(ir::rel::new_source(schema, info))
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

impl ToFromProto for ir::rel::Project {
    type Message = proto::RelProject;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let input = ir::rel::LogicalPlan::from_proto(*non_null!(message.input))?;
        let projections = from_protos(message.projections)?;
        Ok(ir::rel::new_project(input, projections))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let input = self.input.to_proto()?;
        let projections = to_protos(&self.projection)?;
        Ok(proto::RelProject {
            input: Some(input.into()),
            projections,
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
        Ok(bincode::deserialize(&message.payload)?)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        Ok(Self::Message {
            payload: bincode::serialize(self)?,
        })
    }
}
