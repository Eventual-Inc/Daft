#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use half::bf16;

    use crate::{
        array::DataArray,
        datatypes::{
            DataType, Field, Float32Type, Float64Type, UInt16Type, Utf8Array,
            logical::BFloat16Array,
        },
        series::IntoSeries,
    };

    /// Helper: create a BFloat16Array from f32 values (with optional nulls).
    fn make_bf16_array(name: &str, values: &[Option<f32>]) -> BFloat16Array {
        let u16_iter = values.iter().map(|opt| opt.map(|v| bf16::from_f32(v).to_bits()));
        let physical = DataArray::<UInt16Type>::from_iter(
            Field::new(name, DataType::UInt16),
            u16_iter,
        );
        BFloat16Array::new(Field::new(name, DataType::BFloat16), physical)
    }

    // ---- Type system tests ----

    #[test]
    fn bfloat16_display() {
        assert_eq!(DataType::BFloat16.to_string(), "BFloat16");
    }

    #[test]
    fn bfloat16_to_physical() {
        assert_eq!(DataType::BFloat16.to_physical(), DataType::UInt16);
    }

    #[test]
    fn bfloat16_is_numeric() {
        assert!(DataType::BFloat16.is_numeric());
    }

    #[test]
    fn bfloat16_is_logical() {
        assert!(!DataType::BFloat16.is_physical());
    }

    // ---- Cast tests ----

    #[test]
    fn cast_bfloat16_to_float32() {
        let arr = make_bf16_array("x", &[Some(1.0), Some(-2.5), Some(0.0)]);
        let result = arr.cast(&DataType::Float32).unwrap();
        let f32_arr = result.downcast::<DataArray<Float32Type>>().unwrap();

        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        // bf16 can represent 1.0, 0.0 exactly; -2.5 is also exact in bf16
        assert_eq!(values[0], Some(1.0));
        assert_eq!(values[1], Some(-2.5));
        assert_eq!(values[2], Some(0.0));
    }

    #[test]
    fn cast_float32_to_bfloat16() {
        // 1.0009766 in f32 should truncate to 1.0 in bf16 (7-bit mantissa)
        let f32_data = DataArray::<Float32Type>::from_iter(
            Field::new("x", DataType::Float32),
            vec![Some(1.0009766_f32), Some(3.14_f32)],
        );
        let result = f32_data.cast(&DataType::BFloat16).unwrap();
        assert_eq!(result.data_type(), &DataType::BFloat16);

        // Round-trip back to f32 to verify truncation
        let back = result.cast(&DataType::Float32).unwrap();
        let f32_back = back.downcast::<DataArray<Float32Type>>().unwrap();
        let vals: Vec<Option<f32>> = f32_back.into_iter().collect();

        // 1.0009766 truncates to 1.0 in bf16
        assert_eq!(vals[0], Some(1.0));
        // 3.14 truncates to bf16 precision
        let expected_pi = bf16::from_f32(3.14).to_f32();
        assert_eq!(vals[1], Some(expected_pi));
    }

    #[test]
    fn cast_bfloat16_to_float64() {
        let arr = make_bf16_array("x", &[Some(42.0), Some(-1.0)]);
        let result = arr.cast(&DataType::Float64).unwrap();
        let f64_arr = result.downcast::<DataArray<Float64Type>>().unwrap();
        let values: Vec<Option<f64>> = f64_arr.into_iter().collect();
        assert_eq!(values[0], Some(42.0));
        assert_eq!(values[1], Some(-1.0));
    }

    #[test]
    fn cast_bfloat16_to_utf8() {
        let arr = make_bf16_array("x", &[Some(1.5), None, Some(-0.0)]);
        let result = arr.cast(&DataType::Utf8).unwrap();
        let utf8_arr = result.downcast::<Utf8Array>().unwrap();

        assert_eq!(utf8_arr.get(0), Some("1.5"));
        assert_eq!(utf8_arr.get(1), None);
        // -0.0 in bf16 displays as "-0"
        let neg_zero_str = utf8_arr.get(2).unwrap();
        assert!(neg_zero_str == "-0" || neg_zero_str == "-0.0" || neg_zero_str == "0",
            "unexpected -0.0 repr: {neg_zero_str}");
    }

    #[test]
    fn cast_bfloat16_roundtrip() {
        let original_values = vec![Some(1.0_f32), Some(100.0), Some(-0.5), Some(0.0)];
        let arr = make_bf16_array("x", &original_values);

        // bf16 -> f32 -> bf16 should be lossless
        let f32_series = arr.cast(&DataType::Float32).unwrap();
        let back = f32_series.cast(&DataType::BFloat16).unwrap();
        let back_f32 = back.cast(&DataType::Float32).unwrap();
        let back_arr = back_f32.downcast::<DataArray<Float32Type>>().unwrap();

        let round_tripped: Vec<Option<f32>> = back_arr.into_iter().collect();
        for (orig, rt) in original_values.iter().zip(round_tripped.iter()) {
            match (orig, rt) {
                (Some(a), Some(b)) => {
                    let expected = bf16::from_f32(*a).to_f32();
                    assert_eq!(*b, expected, "roundtrip mismatch for {a}");
                }
                (None, None) => {}
                _ => panic!("null mismatch"),
            }
        }
    }

    #[test]
    fn cast_bfloat16_special_values() {
        let arr = make_bf16_array(
            "x",
            &[Some(f32::NAN), Some(f32::INFINITY), Some(f32::NEG_INFINITY)],
        );
        let result = arr.cast(&DataType::Float32).unwrap();
        let f32_arr = result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();

        assert!(values[0].unwrap().is_nan());
        assert_eq!(values[1], Some(f32::INFINITY));
        assert_eq!(values[2], Some(f32::NEG_INFINITY));
    }

    #[test]
    fn cast_bfloat16_with_nulls() {
        let arr = make_bf16_array("x", &[Some(1.0), None, Some(3.0), None]);
        assert_eq!(arr.len(), 4);

        let result = arr.cast(&DataType::Float32).unwrap();
        let f32_arr = result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();

        assert_eq!(values, vec![Some(1.0), None, Some(3.0), None]);
    }

    #[test]
    fn cast_bfloat16_to_int32() {
        let arr = make_bf16_array("x", &[Some(3.7), Some(-1.2), Some(0.0)]);
        let result = arr.cast(&DataType::Int32).unwrap();
        let i32_arr = result
            .downcast::<DataArray<crate::datatypes::Int32Type>>()
            .unwrap();
        let values: Vec<Option<i32>> = i32_arr.into_iter().collect();
        // bf16(3.7) -> f32 -> i32 truncation
        let expected_0 = bf16::from_f32(3.7).to_f32() as i32;
        let expected_1 = bf16::from_f32(-1.2).to_f32() as i32;
        assert_eq!(values[0], Some(expected_0));
        assert_eq!(values[1], Some(expected_1));
        assert_eq!(values[2], Some(0));
    }

    #[test]
    fn cast_int32_to_bfloat16() {
        let i32_data = DataArray::<crate::datatypes::Int32Type>::from_iter(
            Field::new("x", DataType::Int32),
            vec![Some(42_i32), Some(-7), Some(0)],
        );
        let result = i32_data.cast(&DataType::BFloat16).unwrap();
        assert_eq!(result.data_type(), &DataType::BFloat16);

        // Verify via round-trip to f32
        let f32_result = result.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values[0], Some(42.0));
        assert_eq!(values[1], Some(-7.0));
        assert_eq!(values[2], Some(0.0));
    }

    // ---- Repr tests ----

    #[test]
    fn bfloat16_str_value() {
        let arr = make_bf16_array("x", &[Some(1.5), None, Some(f32::INFINITY)]);
        assert_eq!(arr.str_value(0).unwrap(), "1.5");
        assert_eq!(arr.str_value(1).unwrap(), "None");
        assert_eq!(arr.str_value(2).unwrap(), "inf");
    }

    // ---- Sort tests ----

    #[test]
    fn bfloat16_sort_ascending() {
        let arr = make_bf16_array("x", &[Some(3.0), Some(1.0), Some(2.0)]);
        let sorted = arr.sort(false, false).unwrap();

        let f32_result = sorted.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values, vec![Some(1.0), Some(2.0), Some(3.0)]);
    }

    #[test]
    fn bfloat16_sort_descending() {
        let arr = make_bf16_array("x", &[Some(3.0), Some(1.0), Some(2.0)]);
        let sorted = arr.sort(true, false).unwrap();

        let f32_result = sorted.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values, vec![Some(3.0), Some(2.0), Some(1.0)]);
    }

    #[test]
    fn bfloat16_sort_with_negatives() {
        // This test verifies that sorting doesn't use raw u16 ordering,
        // which would incorrectly sort negative bf16 values.
        let arr = make_bf16_array("x", &[Some(1.0), Some(-2.0), Some(-1.0), Some(0.0)]);
        let sorted = arr.sort(false, false).unwrap();

        let f32_result = sorted.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values, vec![Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)]);
    }

    #[test]
    fn bfloat16_sort_with_nulls() {
        let arr = make_bf16_array("x", &[Some(2.0), None, Some(1.0)]);

        // nulls last (default)
        let sorted = arr.sort(false, false).unwrap();
        let f32_result = sorted.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values, vec![Some(1.0), Some(2.0), None]);

        // nulls first
        let sorted = arr.sort(false, true).unwrap();
        let f32_result = sorted.cast(&DataType::Float32).unwrap();
        let f32_arr = f32_result.downcast::<DataArray<Float32Type>>().unwrap();
        let values: Vec<Option<f32>> = f32_arr.into_iter().collect();
        assert_eq!(values, vec![None, Some(1.0), Some(2.0)]);
    }

    #[test]
    fn bfloat16_cast_to_self() {
        let arr = make_bf16_array("x", &[Some(1.0), Some(2.0)]);
        let result = arr.cast(&DataType::BFloat16).unwrap();
        assert_eq!(result.data_type(), &DataType::BFloat16);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn bfloat16_cast_to_null() {
        let arr = make_bf16_array("x", &[Some(1.0), Some(2.0)]);
        let result = arr.cast(&DataType::Null).unwrap();
        assert_eq!(result.data_type(), &DataType::Null);
        assert_eq!(result.len(), 2);
    }
}
