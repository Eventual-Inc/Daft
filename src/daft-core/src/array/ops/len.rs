use std::cmp::min;

#[cfg(feature = "python")]
use common_py_serde::pickle_dumps;
use rand::{SeedableRng, rngs::StdRng};

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{DaftArrowBackedType, FileArray},
    file::DaftMediaType,
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn size_bytes(&self) -> usize {
        daft_arrow::compute::aggregate::estimated_bytes_size(self.data())
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    /// Estimate the size of this list by sampling and pickling its objects.
    pub fn size_bytes(&self) -> usize {
        use pyo3::Python;
        use rand::seq::IndexedRandom;

        // Sample up to 1MB or 10000 items to determine total size.
        const MAX_SAMPLE_QUANTITY: usize = 10000;
        const MAX_SAMPLE_SIZE: usize = 1024 * 1024;

        if self.is_empty() {
            return 0;
        }

        let values = self.values();

        let mut rng = StdRng::seed_from_u64(0);
        let sample_candidates =
            values.choose_multiple(&mut rng, min(values.len(), MAX_SAMPLE_QUANTITY));

        let mut sample_size_allowed = MAX_SAMPLE_SIZE;
        let mut sampled_sizes = Vec::with_capacity(sample_candidates.len());
        Python::attach(|py| {
            for c in sample_candidates {
                // Just estimate to 0 if pickle_dumps fails.
                let size = pickle_dumps(py, c).map(|v| v.len()).unwrap_or(0);
                sampled_sizes.push(size);
                sample_size_allowed = sample_size_allowed.saturating_sub(size);

                if sample_size_allowed == 0 {
                    break;
                }
            }
        });

        if sampled_sizes.len() == values.len() {
            // Sampling complete.
            // If we ended up measuring the entire list, just return the exact value.

            sampled_sizes.into_iter().sum()
        } else {
            // Otherwise, reduce to a one-item estimate and extrapolate.

            let one_item_size_estimate = if sampled_sizes.len() == 1 {
                sampled_sizes[0]
            } else {
                let sampled_len = sampled_sizes.len() as f64;

                let mean: f64 = sampled_sizes.iter().map(|&x| x as f64).sum::<f64>() / sampled_len;
                let stdev: f64 = sampled_sizes
                    .iter()
                    .map(|&x| ((x as f64) - mean).powi(2))
                    .sum::<f64>()
                    / sampled_len;

                (mean + stdev) as usize
            };

            one_item_size_estimate * values.len()
        }
    }
}

/// From arrow2 private method (arrow2::compute::aggregate::validity_size)
fn validity_size(validity: Option<&daft_arrow::buffer::NullBuffer>) -> usize {
    validity.map(|b| b.buffer().len()).unwrap_or(0)
}

fn offset_size(offsets: &daft_arrow::offset::OffsetsBuffer<i64>) -> usize {
    offsets.len_proxy() * std::mem::size_of::<i64>()
}

impl FixedSizeListArray {
    pub fn size_bytes(&self) -> usize {
        self.flat_child.size_bytes() + validity_size(self.validity())
    }
}

impl ListArray {
    pub fn size_bytes(&self) -> usize {
        self.flat_child.size_bytes() + validity_size(self.validity()) + offset_size(self.offsets())
    }
}

impl StructArray {
    pub fn size_bytes(&self) -> usize {
        let children_size_bytes: usize = self.children.iter().map(|s| s.size_bytes()).sum();
        children_size_bytes + validity_size(self.validity())
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn size_bytes(&self) -> usize {
        self.physical.size_bytes()
    }
}
