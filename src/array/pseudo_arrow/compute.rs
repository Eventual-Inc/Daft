use crate::array::pseudo_arrow::PseudoArrowArray;

impl<T: Clone> PseudoArrowArray<T> {
    pub fn concatenate(_arrays: Vec<&Self>) -> Self {
        todo!()
        // let mut concatenated_values: Vec<T> = Vec::new();
        // for array in arrays {
        //     concatenated_values.extend_from_slice(array.vec());
        // }
        // VecBackedArray::new(concatenated_values)
    }
}
