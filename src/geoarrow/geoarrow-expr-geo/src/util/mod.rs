use std::sync::Arc;

use geoarrow_array::GeoArrowArray;

pub(crate) mod downcast;
pub mod to_geo;

pub(crate) fn copy_geoarrow_array_ref(array: &dyn GeoArrowArray) -> Arc<dyn GeoArrowArray> {
    array.slice(0, array.len())
}
