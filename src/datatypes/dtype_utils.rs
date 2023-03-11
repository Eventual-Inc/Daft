use super::dtype::DataType;
use crate::error::DaftResult;

#[inline]
pub fn is_numeric(dt: &DataType) -> bool {
    match dt {
         DataType::Int8
         | DataType::Int16
         | DataType::Int32
         | DataType::Int64
         | DataType::UInt8
         | DataType::UInt16
         | DataType::UInt32
         | DataType::UInt64
         // DataType::Float16
         | DataType::Float32
         | DataType::Float64 => true,
         _ => false
     }
}

#[inline]
pub fn is_castable(dt: &DataType, cast_to: &DataType) -> DaftResult<bool> {
    Ok(arrow2::compute::cast::can_cast_types(
        &dt.to_arrow()?,
        &cast_to.to_arrow()?,
    ))
}
