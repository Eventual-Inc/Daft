use arrow_array::RecordBatchReader;
use geozero::{FeatureProcessor, GeozeroDatasource, error::GeozeroError};

use crate::geozero::export::data_source::record_batch::{geometry_columns, process_batch};

/// A newtype wrapper around an [`arrow_array::RecordBatchReader`] so that we can implement the
/// [`geozero::GeozeroDatasource`] trait on it.
///
/// This allows for exporting Arrow data to a geozero-based consumer even when not all of the Arrow
/// data is present in memory at once.
pub struct GeozeroRecordBatchReader(Box<dyn RecordBatchReader>);

impl GeozeroRecordBatchReader {
    /// Create a new GeozeroRecordBatchReader from a [`RecordBatchReader`].
    pub fn new(reader: Box<dyn RecordBatchReader>) -> Self {
        Self(reader)
    }

    /// Access the underlying [`RecordBatchReader`].
    pub fn into_inner(self) -> Box<dyn RecordBatchReader> {
        self.0
    }
}

impl AsRef<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn as_ref(&self) -> &Box<dyn RecordBatchReader> {
        &self.0
    }
}

impl AsMut<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn as_mut(&mut self) -> &mut Box<dyn RecordBatchReader> {
        &mut self.0
    }
}

impl From<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader>) -> Self {
        Self(value)
    }
}

impl From<Box<dyn RecordBatchReader + Send>> for GeozeroRecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader + Send>) -> Self {
        Self(value)
    }
}

impl GeozeroDatasource for GeozeroRecordBatchReader {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<(), GeozeroError> {
        let reader = self.as_mut();
        let schema = reader.schema();
        let geom_indices = geometry_columns(&schema);
        let geometry_column_index = if geom_indices.len() != 1 {
            Err(GeozeroError::Dataset(
                "Writing through geozero not supported with multiple geometries".to_string(),
            ))?
        } else {
            geom_indices[0]
        };

        processor.dataset_begin(None)?;

        let mut overall_row_idx = 0;
        for batch in reader.into_iter() {
            let batch = batch.map_err(|err| GeozeroError::Dataset(err.to_string()))?;
            process_batch(
                &batch,
                &schema,
                geometry_column_index,
                overall_row_idx,
                processor,
            )?;
            overall_row_idx += batch.num_rows();
        }

        processor.dataset_end()?;

        Ok(())
    }
}
