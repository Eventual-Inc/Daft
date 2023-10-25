use std::fmt::{write, Display};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;

use crate::{DataFileSource, FileType, ScanOperator, ScanOperatorRef, ScanTask};
#[derive(Debug)]
pub struct AnonymousScanOperator {
    schema: SchemaRef,
    file_type: FileType,
    files: Vec<String>,
    columns_to_select: Option<Vec<String>>,
    limit: Option<usize>,
}

impl AnonymousScanOperator {
    pub fn new(schema: SchemaRef, file_type: FileType, files: Vec<String>) -> Self {
        Self {
            schema,
            file_type,
            files,
            columns_to_select: None,
            limit: None,
        }
    }
}

impl Display for AnonymousScanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl ScanOperator for AnonymousScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[daft_core::datatypes::Field] {
        &[]
    }

    fn num_partitions(&self) -> common_error::DaftResult<usize> {
        Ok(self.files.len())
    }

    fn select(self: Box<Self>, columns: &[&str]) -> common_error::DaftResult<ScanOperatorRef> {
        for c in columns {
            if self.schema.get_field(c).is_err() {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "{c} not found in {:?}",
                    self.columns_to_select
                )));
            }
        }
        let mut to_rtn = self;
        to_rtn.columns_to_select = Some(columns.iter().map(|s| s.to_string()).collect());
        Ok(to_rtn)
    }

    fn limit(self: Box<Self>, num: usize) -> DaftResult<ScanOperatorRef> {
        let mut to_rtn = self;
        to_rtn.limit = Some(num);
        Ok(to_rtn)
    }

    fn filter(self: Box<Self>, _predicate: &daft_dsl::Expr) -> DaftResult<(bool, ScanOperatorRef)> {
        Ok((false, self))
    }

    fn to_scan_tasks(
        self: Box<Self>,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<crate::ScanTask>>>> {
        let iter = self.files.clone().into_iter().map(move |f| {
            let source = DataFileSource::AnonymousDataFile {
                file_type: self.file_type,
                path: f,
                metadata: None,
                partition_spec: None,
                statistics: None,
            };
            Ok(ScanTask {
                source,
                columns: self.columns_to_select.clone(),
                limit: self.limit,
            })
        });
        Ok(Box::new(iter))
    }
}
