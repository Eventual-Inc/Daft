use std::ops::Not;

use crate::array::pseudo_arrow::PseudoArrowArray;
use arrow2::array::Array;
use arrow2::bitmap::Bitmap;

impl<T: Send + Sync + Clone + 'static> PseudoArrowArray<T> {
    pub fn concatenate(arrays: Vec<&Self>) -> Self {
        // Concatenate the values and the validity separately.

        let mut concatenated_values: Vec<T> = Vec::new();
        for array in arrays.iter() {
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

        PseudoArrowArray::new(concatenated_values.into(), Some(concatenated_validity))
    }
}
