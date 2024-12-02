use crate::{
    array::{Array, BooleanArray, MutableArray, MutableBooleanArray, MutablePrimitiveArray},
    bitmap::Bitmap,
    datatypes::DataType,
};

/// Zip two arrays by some boolean mask. Where the mask evaluates `true` values of `true_values`
/// are taken, where the mask evaluates `false` values of `false_values` are taken.
///
/// # Arguments
/// * `mask` - Boolean values used to determine from which array to take the values.
/// * `true_values` - Values of this array are taken if mask evaluates `true`
/// * `false_values` - Values of this array are taken if mask evaluates `false`
pub fn zip(
    mask: &Bitmap,
    true_values: &dyn Array,
    false_values: &dyn Array,
) -> crate::error::Result<Box<dyn Array>> {
    if true_values.data_type() != false_values.data_type() {
        return Err(crate::error::Error::InvalidArgumentError(
            "arguments need to have the same data type".into(),
        ));
    }
    macro_rules! zip {
        ($ty:ty, $out:ty) => {{
            let true_values = true_values.as_any().downcast_ref::<$ty>().unwrap();
            let false_values = false_values.as_any().downcast_ref::<$ty>().unwrap();

            let mut mutable = <$out>::with_capacity(true_values.len());
            mask.iter().enumerate().for_each(|(i, x)| {
                if x {
                    let value = true_values.get(i);
                    mutable.push(value)
                } else {
                    let value = false_values.get(i);
                    mutable.push(value)
                }
            });

            mutable.as_box()
        }};
    }
    macro_rules! zip_primitive {
        ($ty:ty, $native:ty) => {{
            let true_values = true_values.as_any().downcast_ref::<$ty>().unwrap();
            let false_values = false_values.as_any().downcast_ref::<$ty>().unwrap();

            let mut mutable = MutablePrimitiveArray::<$native>::with_capacity(true_values.len());
            mask.iter().enumerate().for_each(|(i, x)| {
                if x {
                    let value = true_values.get(i);
                    mutable.push(value)
                } else {
                    let value = false_values.get(i);
                    mutable.push(value)
                }
            });

            mutable.as_box()
        }};
    }

    Ok(match true_values.data_type() {
        crate::datatypes::DataType::Null => true_values.to_boxed(),
        crate::datatypes::DataType::Boolean => {
            zip!(BooleanArray, MutableBooleanArray)
        }
        crate::datatypes::DataType::Int8 => zip_primitive!(crate::array::Int8Array, i8),
        crate::datatypes::DataType::Int16 => zip_primitive!(crate::array::Int16Array, i16),
        crate::datatypes::DataType::Int32 => zip_primitive!(crate::array::Int32Array, i32),
        crate::datatypes::DataType::Int64 => zip_primitive!(crate::array::Int64Array, i64),
        crate::datatypes::DataType::UInt8 => zip_primitive!(crate::array::UInt8Array, u8),
        crate::datatypes::DataType::UInt16 => zip_primitive!(crate::array::UInt16Array, u16),
        crate::datatypes::DataType::UInt32 => zip_primitive!(crate::array::UInt32Array, u32),
        crate::datatypes::DataType::UInt64 => zip_primitive!(crate::array::UInt64Array, u64),
        crate::datatypes::DataType::Float32 => zip_primitive!(crate::array::Float32Array, f32),
        crate::datatypes::DataType::Float64 => zip_primitive!(crate::array::Float64Array, f64),
        crate::datatypes::DataType::Date32 => {
            let mut arr = zip_primitive!(crate::array::Int32Array, i32);
            arr.change_type(DataType::Date32);
            arr
        }
        crate::datatypes::DataType::Date64 => {
            let mut arr = zip_primitive!(crate::array::Int64Array, i64);
            arr.change_type(DataType::Date64);
            arr
        }
        dtype @ crate::datatypes::DataType::Time32(_) => {
            let mut arr = zip_primitive!(crate::array::Int32Array, i32);
            arr.change_type(dtype.clone());
            arr
        }
        dtype @ crate::datatypes::DataType::Time64(_) => {
            let mut arr = zip_primitive!(crate::array::Int64Array, i64);
            arr.change_type(dtype.clone());
            arr
        }

        crate::datatypes::DataType::Binary => {
            zip!(
                crate::array::BinaryArray<i32>,
                crate::array::MutableBinaryArray<i32>
            )
        }
        crate::datatypes::DataType::FixedSizeBinary(size) => {
            let true_values = true_values
                .as_any()
                .downcast_ref::<crate::array::FixedSizeBinaryArray>()
                .unwrap();
            let false_values = false_values
                .as_any()
                .downcast_ref::<crate::array::FixedSizeBinaryArray>()
                .unwrap();
            let mut mutable = <crate::array::MutableFixedSizeBinaryArray>::with_capacity(
                *size,
                true_values.len(),
            );
            mask.iter().enumerate().for_each(|(i, x)| {
                if x {
                    let value = true_values.get(i);
                    mutable.push(value)
                } else {
                    let value = false_values.get(i);
                    mutable.push(value)
                }
            });
            mutable.as_box()
        }
        crate::datatypes::DataType::LargeBinary => {
            zip!(
                crate::array::BinaryArray<i64>,
                crate::array::MutableBinaryArray<i64>
            )
        }
        crate::datatypes::DataType::Utf8 => {
            zip!(
                crate::array::Utf8Array<i32>,
                crate::array::MutableUtf8Array<i32>
            )
        }

        crate::datatypes::DataType::LargeUtf8 => {
            zip!(
                crate::array::Utf8Array<i64>,
                crate::array::MutableUtf8Array<i64>
            )
        }

        dtype => Err(crate::error::Error::nyi(format!(
            "zip does not yet support {:?}",
            dtype
        )))?,
    })
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::array::Int8Array;

    #[test]
    fn test_zip() {
        let mask = Bitmap::from([true, true, false, false, true]);

        let true_values = Int8Array::from(vec![None, Some(2), Some(3), None, Some(5)]).boxed();
        let false_values = Int8Array::from(vec![None, None, Some(10), Some(11), None]).boxed();

        let result = zip(&mask, true_values.as_ref(), false_values.as_ref()).unwrap();
        let expected = Int8Array::from(vec![None, Some(2), Some(10), Some(11), Some(5)]).boxed();

        assert_eq!(result.as_ref(), expected.as_ref());
    }
}
