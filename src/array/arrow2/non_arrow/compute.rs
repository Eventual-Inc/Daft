impl<T: Clone> NonArrowArray<T> {
    pub fn concatenate(arrays: Vec<&Self>) -> Self {
        todo!()
        // let mut concatenated_values: Vec<T> = Vec::new();
        // for array in arrays {
        //     concatenated_values.extend_from_slice(array.vec());
        // }
        // VecBackedArray::new(concatenated_values)
    }
}
