use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
};
use daft_dsl::resolve_exprs;

use crate::{sink_info::SinkInfo, LogicalPlan, OutputFileInfo};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sink {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
    /// Information about the sink data location.
    pub sink_info: Arc<SinkInfo>,
}

impl Sink {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, sink_info: Arc<SinkInfo>) -> DaftResult<Self> {
        let schema = input.schema();

        let sink_info = match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(OutputFileInfo {
                root_dir,
                file_format,
                partition_cols,
                compression,
                io_config,
            }) => {
                let resolved_partition_cols = partition_cols
                    .clone()
                    .map(|cols| {
                        resolve_exprs(cols, &schema).map(|(resolved_cols, _)| resolved_cols)
                    })
                    .transpose()?;

                Arc::new(SinkInfo::OutputFileInfo(OutputFileInfo {
                    root_dir: root_dir.clone(),
                    file_format: file_format.clone(),
                    partition_cols: resolved_partition_cols,
                    compression: compression.clone(),
                    io_config: io_config.clone(),
                }))
            }
            _ => sink_info,
        };

        let fields = match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                let mut fields = vec![Field::new("path", daft_core::DataType::Utf8)];
                if let Some(ref pcols) = output_file_info.partition_cols {
                    for pc in pcols {
                        fields.push(pc.to_field(&schema)?);
                    }
                }
                fields
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(..) => {
                vec![
                    // We have to return datafile since PyIceberg Table is not picklable yet
                    Field::new("data_file", daft_core::DataType::Python),
                ]
            }
        };
        let schema = Schema::new(fields)?.into();
        Ok(Self {
            input,
            schema,
            sink_info,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                res.push(format!("Sink: {:?}", output_file_info.file_format));
                res.extend(output_file_info.multiline_display());
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(catalog_info) => match &catalog_info.catalog {
                crate::sink_info::CatalogType::Iceberg(iceberg_info) => {
                    res.push(format!("Sink: Iceberg({})", iceberg_info.table_name));
                    res.extend(iceberg_info.multiline_display());
                }
                crate::sink_info::CatalogType::DeltaLake(deltalake_info) => {
                    res.push(format!("Sink: DeltaLake({})", deltalake_info.path));
                    res.extend(deltalake_info.multiline_display());
                }
                crate::sink_info::CatalogType::Lance(lance_info) => {
                    res.push(format!("Sink: Lance({})", lance_info.path));
                    res.extend(lance_info.multiline_display());
                }
            },
        }
        res.push(format!("Output schema = {}", self.schema.short_string()));
        res
    }
}
