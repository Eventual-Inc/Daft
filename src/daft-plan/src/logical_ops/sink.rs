use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{resolve_exprs, ExprRef};

use crate::{sink_info::SinkInfo, LogicalPlan, OutputFileInfo};
#[cfg(feature = "python")]
use crate::{
    sink_info::{CatalogInfo, CatalogType},
    DeltaLakeCatalogInfo,
};

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

        fn resolve_partition_cols(
            partition_cols: &Option<Vec<ExprRef>>,
            schema: &Schema,
        ) -> DaftResult<Option<Vec<ExprRef>>> {
            partition_cols
                .clone()
                .map(|cols| {
                    resolve_exprs(cols, schema, false).map(|(resolved_cols, _)| resolved_cols)
                })
                .transpose()
        }

        // replace partition columns with resolved columns
        let sink_info = match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(OutputFileInfo {
                root_dir,
                file_format,
                partition_cols,
                compression,
                io_config,
            }) => Arc::new(SinkInfo::OutputFileInfo(OutputFileInfo {
                root_dir: root_dir.clone(),
                file_format: *file_format,
                partition_cols: resolve_partition_cols(partition_cols, &schema)?,
                compression: compression.clone(),
                io_config: io_config.clone(),
            })),
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(CatalogInfo {
                catalog,
                catalog_columns,
            }) => match catalog {
                CatalogType::Iceberg(_) | CatalogType::Lance(_) => sink_info,
                CatalogType::DeltaLake(DeltaLakeCatalogInfo {
                    path,
                    mode,
                    version,
                    large_dtypes,
                    partition_cols,
                    io_config,
                }) => Arc::new(SinkInfo::CatalogInfo(CatalogInfo {
                    catalog: CatalogType::DeltaLake(DeltaLakeCatalogInfo {
                        path: path.clone(),
                        mode: mode.clone(),
                        version: *version,
                        large_dtypes: *large_dtypes,
                        partition_cols: resolve_partition_cols(partition_cols, &schema)?,
                        io_config: io_config.clone(),
                    }),
                    catalog_columns: catalog_columns.clone(),
                })),
            },
        };

        let fields = match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                let mut fields = vec![Field::new("path", DataType::Utf8)];
                if let Some(ref pcols) = output_file_info.partition_cols {
                    for pc in pcols {
                        fields.push(pc.to_field(&schema)?);
                    }
                }
                fields
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(catalog_info) => {
                match catalog_info.catalog {
                    CatalogType::Iceberg(_) => {
                        vec![
                            // We have to return datafile since PyIceberg Table is not picklable yet
                            Field::new("data_file", DataType::Python),
                        ]
                    }
                    CatalogType::DeltaLake(_) => vec![Field::new("add_action", DataType::Python)],
                    CatalogType::Lance(_) => vec![Field::new("fragments", DataType::Python)],
                }
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
                CatalogType::Iceberg(iceberg_info) => {
                    res.push(format!("Sink: Iceberg({})", iceberg_info.table_name));
                    res.extend(iceberg_info.multiline_display());
                }
                CatalogType::DeltaLake(deltalake_info) => {
                    res.push(format!("Sink: DeltaLake({})", deltalake_info.path));
                    res.extend(deltalake_info.multiline_display());
                }
                CatalogType::Lance(lance_info) => {
                    res.push(format!("Sink: Lance({})", lance_info.path));
                    res.extend(lance_info.multiline_display());
                }
            },
        }
        res.push(format!("Output schema = {}", self.schema.short_string()));
        res
    }
}
