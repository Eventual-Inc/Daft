use crate::array::ListArray;
use crate::datatypes::logical::{COOSparseTensorArray, FixedShapeCOOSparseTensorArray};

impl COOSparseTensorArray {
    pub fn values_array(&self) -> &ListArray {
        const VALUES_IDX: usize = 0;
        let array = self.physical.children.get(VALUES_IDX).unwrap();
        array.list().unwrap()
    }
    
    pub fn indices_array(&self) -> &ListArray {
        const INDICES_IDX: usize = 1;
        let array = self.physical.children.get(INDICES_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn shape_array(&self) -> &ListArray {
        const SHAPE_IDX: usize = 2;
        let array = self.physical.children.get(SHAPE_IDX).unwrap();
        array.list().unwrap()
    }
}

impl FixedShapeCOOSparseTensorArray {
    pub fn values_array(&self) -> &ListArray {
        const VALUES_IDX: usize = 0;
        let array = self.physical.children.get(VALUES_IDX).unwrap();
        array.list().unwrap()
    }
    
    pub fn indices_array(&self) -> &ListArray {
        const INDICES_IDX: usize = 1;
        let array = self.physical.children.get(INDICES_IDX).unwrap();
        array.list().unwrap()
    }
}