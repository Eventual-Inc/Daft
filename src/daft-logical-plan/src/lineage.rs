use common_file_formats::FileFormat;
use serde::Serialize;

use crate::{
    LogicalPlan,
    ops::{Sink, Source},
    sink_info::SinkInfo,
    source_info::SourceInfo,
};

/// Plan-level lineage: the external input and output datasets a logical plan
/// references.
///
/// Derived by walking the plan tree (see [`LogicalPlan::lineage`]) and exposed
/// through the plan's JSON representation, so subscribers can consume it without
/// reaching into internal plan structures.
#[derive(Debug, Default, Serialize)]
pub struct Lineage {
    pub inputs: Vec<LineageInput>,
    pub outputs: Vec<LineageOutput>,
}

/// An input dataset, extracted from a source node.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LineageInput {
    /// File paths only materialize into scan tasks after planning, so at the
    /// logical level the operator name is the best available identifier.
    PhysicalScan {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    GlobScan {
        glob_paths: Vec<String>,
    },
    InMemory,
}

/// An output dataset, extracted from a sink node.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LineageOutput {
    File {
        root_dir: String,
        file_format: FileFormat,
    },
    #[cfg(feature = "python")]
    Iceberg { name: String, location: String },
    #[cfg(feature = "python")]
    DeltaLake { path: String },
    #[cfg(feature = "python")]
    Lance { path: String },
    #[cfg(feature = "python")]
    DataSink { name: String },
}

impl LineageInput {
    /// Returns `None` for internal placeholder sources, which don't map to a
    /// real external dataset.
    fn from_source(source: &Source) -> Option<Self> {
        match source.source_info.as_ref() {
            SourceInfo::Physical(info) => {
                let name = match &info.scan_state {
                    daft_scan::ScanState::Operator(scan_op) => Some(scan_op.0.name().to_string()),
                    daft_scan::ScanState::Tasks(_) => None,
                };
                Some(Self::PhysicalScan { name })
            }
            SourceInfo::GlobScan(info) => Some(Self::GlobScan {
                glob_paths: info.glob_paths.as_ref().clone(),
            }),
            SourceInfo::InMemory(_) => Some(Self::InMemory),
            SourceInfo::PlaceHolder(_) => None,
        }
    }
}

impl LineageOutput {
    fn from_sink(sink: &Sink) -> Option<Self> {
        match sink.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(info) => Some(Self::File {
                root_dir: info.root_dir.clone(),
                file_format: info.file_format,
            }),
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(info) => {
                use crate::sink_info::CatalogType;
                Some(match &info.catalog {
                    CatalogType::Iceberg(iceberg) => Self::Iceberg {
                        name: iceberg.table_name.clone(),
                        location: iceberg.table_location.clone(),
                    },
                    CatalogType::DeltaLake(delta) => Self::DeltaLake {
                        path: delta.path.clone(),
                    },
                    CatalogType::Lance(lance) => Self::Lance {
                        path: lance.path.clone(),
                    },
                })
            }
            #[cfg(feature = "python")]
            SinkInfo::DataSinkInfo(info) => Some(Self::DataSink {
                name: info.name.clone(),
            }),
        }
    }
}

impl LogicalPlan {
    /// Derive plan-level lineage by walking the plan tree, collecting input
    /// datasets from source nodes and output datasets from sink nodes.
    pub fn lineage(&self) -> Lineage {
        let mut lineage = Lineage::default();
        self.collect_lineage(&mut lineage);
        lineage
    }

    fn collect_lineage(&self, lineage: &mut Lineage) {
        match self {
            Self::Source(source) => {
                if let Some(input) = LineageInput::from_source(source) {
                    lineage.inputs.push(input);
                }
            }
            Self::Sink(sink) => {
                if let Some(output) = LineageOutput::from_sink(sink) {
                    lineage.outputs.push(output);
                }
            }
            _ => {}
        }
        for child in self.children() {
            child.collect_lineage(lineage);
        }
    }
}
