use std::sync::Arc;

use daft_core::{datatypes::Field, schema::Schema};

use crate::{
    ops::Source,
    source_info::{ExternalInfo, FileFormatConfig, FileInfo, SourceInfo},
    JsonSourceConfig, PartitionSpec,
};

pub fn dummy_scan_node(fields: Vec<Field>) -> Source {
    let schema = Arc::new(Schema::new(fields).unwrap());
    Source::new(
        schema.clone(),
        SourceInfo::ExternalInfo(ExternalInfo::new(
            schema.clone(),
            FileInfo::new(vec!["/foo".to_string()], vec![None], vec![None]).into(),
            FileFormatConfig::Json(JsonSourceConfig {}).into(),
        ))
        .into(),
        PartitionSpec::default().into(),
    )
}
