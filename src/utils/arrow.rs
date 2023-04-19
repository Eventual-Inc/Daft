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
                arrow2::datatypes::DataType::LargeList(field.clone()),
                offsets,
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
