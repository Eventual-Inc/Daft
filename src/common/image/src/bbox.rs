#[derive(Clone)]
pub struct BBox(pub u32, pub u32, pub u32, pub u32);

impl BBox {
    pub fn from_u32_arrow_array(arr: &dyn arrow2::array::Array) -> Self {
        assert!(arr.len() == 4);
        let mut iter = arr
            .as_any()
            .downcast_ref::<arrow2::array::UInt32Array>()
            .unwrap()
            .iter();
        Self(
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
        )
    }
}
