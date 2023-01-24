mod from;
mod ops;

use std::sync::Arc;

use crate::{
    array::BaseArray,
    datatypes::{DataType, Field},
};

#[derive(Debug, Clone)]
pub struct Series {
    data_array: Arc<dyn BaseArray>,
}

impl Series {
    pub fn new(data_array: Arc<dyn BaseArray>) -> Self {
        Series { data_array }
    }

    pub fn array(&self) -> &dyn BaseArray {
        self.data_array.as_ref()
    }

    pub fn data_type(&self) -> &DataType {
        self.data_array.data_type()
    }

    pub fn name(&self) -> &str {
        self.data_array.name()
    }

    pub fn len(&self) -> usize {
        self.data_array.len()
    }

    pub fn field(&self) -> &Field {
        self.data_array.field()
    }
}

#[cfg(test)]
mod tests {}
