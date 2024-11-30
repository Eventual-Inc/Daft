use std::ops::Not;

use arrow2::{array::Array, bitmap::Bitmap};

use crate::array::pseudo_arrow::PseudoArrowArray;

impl<T: Send + Sync + Clone + 'static> PseudoArrowArray<T> {
    pub fn concatenate(arrays: Vec<&Self>) -> Self {
        // Concatenate the values and the validity separately.

        let mut concatenated_values: Vec<T> = Vec::new();
        for array in &arrays {
            concatenated_values.extend_from_slice(array.values());
        }

        let bitmaps: Vec<Bitmap> = arrays
            .iter()
            .map(|array| {
                array
                    .validity()
                    .cloned()
                    .unwrap_or_else(|| arrow2::bitmap::Bitmap::new_zeroed(array.len()).not())
            })
            .collect();

        let concatenated_validity =
            Bitmap::from_iter(bitmaps.iter().flat_map(|bitmap| bitmap.iter()));

        Self::new(concatenated_values.into(), Some(concatenated_validity))
    }
}
