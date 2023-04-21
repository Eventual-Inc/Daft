use arrow2::compute::cast;

pub fn cast_array_if_needed(
    arrow_array: Box<dyn arrow2::array::Array>,
) -> Box<dyn arrow2::array::Array> {
    match arrow_array.data_type() {
        arrow2::datatypes::DataType::Utf8 => {
            cast::utf8_to_large_utf8(arrow_array.as_any().downcast_ref().unwrap()).boxed()
        }
        arrow2::datatypes::DataType::Binary => cast::binary_to_large_binary(
            arrow_array.as_any().downcast_ref().unwrap(),
            arrow2::datatypes::DataType::LargeBinary,
        )
        .boxed(),
        arrow2::datatypes::DataType::List(field) => {
            let array = arrow_array
                .as_any()
                .downcast_ref::<arrow2::array::ListArray<i32>>()
                .unwrap();
            let new_values = cast_array_if_needed(array.values().clone());
            let offsets = array.offsets().into();
            arrow2::array::ListArray::<i64>::new(
                arrow2::datatypes::DataType::LargeList(Box::new(arrow2::datatypes::Field::new(
                    field.name.clone(),
                    new_values.data_type().clone(),
                    field.is_nullable,
                ))),
                offsets,
                new_values,
                arrow_array.validity().cloned(),
            )
            .boxed()
        }
        arrow2::datatypes::DataType::LargeList(field) => {
            // Types nested within LargeList may need casting.
            let array = arrow_array
                .as_any()
                .downcast_ref::<arrow2::array::ListArray<i64>>()
                .unwrap();
            let new_values = cast_array_if_needed(array.values().clone());
            if new_values.data_type() == array.values().data_type() {
                return arrow_array;
            }
            arrow2::array::ListArray::<i64>::new(
                arrow2::datatypes::DataType::LargeList(Box::new(arrow2::datatypes::Field::new(
                    field.name.clone(),
                    new_values.data_type().clone(),
                    field.is_nullable,
                ))),
                array.offsets().clone(),
                new_values,
                arrow_array.validity().cloned(),
            )
            .boxed()
        }
        arrow2::datatypes::DataType::FixedSizeList(field, size) => {
            // Types nested within FixedSizeList may need casting.
            let array = arrow_array
                .as_any()
                .downcast_ref::<arrow2::array::FixedSizeListArray>()
                .unwrap();
            let new_values = cast_array_if_needed(array.values().clone());
            if new_values.data_type() == array.values().data_type() {
                return arrow_array;
            }
            arrow2::array::FixedSizeListArray::new(
                arrow2::datatypes::DataType::FixedSizeList(
                    Box::new(arrow2::datatypes::Field::new(
                        field.name.clone(),
                        new_values.data_type().clone(),
                        field.is_nullable,
                    )),
                    *size,
                ),
                new_values,
                arrow_array.validity().cloned(),
            )
            .boxed()
        }
        arrow2::datatypes::DataType::Struct(fields) => {
            let new_arrays = arrow_array
                .as_any()
                .downcast_ref::<arrow2::array::StructArray>()
                .unwrap()
                .values()
                .iter()
                .map(|field_arr| cast_array_if_needed(field_arr.clone()))
                .collect::<Vec<Box<dyn arrow2::array::Array>>>();
            let new_fields = fields
                .iter()
                .zip(new_arrays.iter().map(|arr| arr.data_type().clone()))
                .map(|(field, dtype)| {
                    arrow2::datatypes::Field::new(field.name.clone(), dtype, field.is_nullable)
                })
                .collect();
            Box::new(arrow2::array::StructArray::new(
                arrow2::datatypes::DataType::Struct(new_fields),
                new_arrays,
                arrow_array.validity().cloned(),
            ))
        }
        _ => arrow_array,
    }
}

#[inline]
pub fn arrow_bitmap_and_helper(
    l_bitmap: Option<&arrow2::bitmap::Bitmap>,
    r_bitmap: Option<&arrow2::bitmap::Bitmap>,
) -> Option<arrow2::bitmap::Bitmap> {
    match (l_bitmap, r_bitmap) {
        (None, None) => None,
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(l), Some(r)) => Some(arrow2::bitmap::and(l, r)),
    }
}
