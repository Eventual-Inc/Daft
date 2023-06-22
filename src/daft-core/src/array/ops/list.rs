use crate::datatypes::{FixedSizeListArray, ListArray, UInt64Array, Utf8Array};

use crate::series::Series;

use arrow2;
use arrow2::array::Array;

use common_error::DaftResult;

use super::as_arrow::AsArrow;

fn join_arrow_list_of_utf8s(
    list_element: Option<Box<dyn arrow2::array::Array>>,
    delimiter_str: &str,
) -> Option<String> {
    list_element
        .map(|list_element| {
            list_element
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap()
                .iter()
                .fold(String::from(""), |acc, str_item| {
                    acc + str_item.unwrap_or("") + delimiter_str
                })
            // Remove trailing `delimiter_str`
        })
        .map(|result| {
            let result_len = result.len();
            if result_len > 0 {
                result[..result_len - delimiter_str.len()].to_string()
            } else {
                result
            }
        })
}

impl ListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let list_array = self.as_arrow();
        let offsets = list_array.offsets();

        let mut lens = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            lens.push((unsafe { offsets.get_unchecked(i + 1) - offsets.get_unchecked(i) }) as u64)
        }
        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(lens)
                .with_validity(list_array.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    pub fn explode(&self) -> DaftResult<Series> {
        let list_array = self.as_arrow();
        let child_array = list_array.values().as_ref();
        let offsets = list_array.offsets();

        let total_capacity: i64 = (0..list_array.len())
            .map(|i| {
                let is_valid = list_array.is_valid(i);
                let len: i64 = offsets.get(i + 1).unwrap() - offsets.get(i).unwrap();
                match (is_valid, len) {
                    (false, _) => 1,
                    (true, 0) => 1,
                    (true, l) => l,
                }
            })
            .sum();
        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i);
            let start = offsets.get(i).unwrap();
            let len = offsets.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => growable.extend_validity(1),
                (true, 0) => growable.extend_validity(1),
                (true, l) => growable.extend(0, *start as usize, l as usize),
            }
        }

        Series::try_from((self.field.name.as_ref(), growable.as_box()))
    }

    pub fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        let list_array = self.as_arrow();
        assert_eq!(
            list_array.values().data_type(),
            &arrow2::datatypes::DataType::LargeUtf8
        );

        if delimiter.len() == 1 {
            let delimiter_str = delimiter.get(0).unwrap();
            let result = list_array
                .iter()
                .map(|list_element| join_arrow_list_of_utf8s(list_element, delimiter_str));
            Ok(Utf8Array::from((
                self.name(),
                Box::new(arrow2::array::Utf8Array::from_iter(result)),
            )))
        } else {
            assert_eq!(delimiter.len(), self.len());
            let result = list_array.iter().zip(delimiter.as_arrow().iter()).map(
                |(list_element, delimiter_element)| {
                    let delimiter_str = delimiter_element.unwrap_or("");
                    join_arrow_list_of_utf8s(list_element, delimiter_str)
                },
            );
            Ok(Utf8Array::from((
                self.name(),
                Box::new(arrow2::array::Utf8Array::from_iter(result)),
            )))
        }
    }
}

impl FixedSizeListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let list_array = self.as_arrow();
        let list_size = list_array.size();
        let lens = (0..self.len())
            .map(|_| list_size as u64)
            .collect::<Vec<_>>();
        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(lens)
                .with_validity(list_array.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    pub fn explode(&self) -> DaftResult<Series> {
        let list_array = self.as_arrow();
        let child_array = list_array.values().as_ref();

        let list_size = list_array.size();

        let mut total_capacity: i64 =
            (list_size * (list_array.len() - list_array.null_count())) as i64;

        if list_size == 0 {
            total_capacity = list_array.len() as i64;
        }

        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i) && (list_size > 0);
            match is_valid {
                false => growable.extend_validity(1),
                true => growable.extend(0, i * list_size, list_size),
            }
        }
        Series::try_from((self.field.name.as_ref(), growable.as_box()))
    }

    pub fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        let list_array = self.as_arrow();
        assert_eq!(
            list_array.values().data_type(),
            &arrow2::datatypes::DataType::LargeUtf8
        );

        if delimiter.len() == 1 {
            let delimiter_str = delimiter.get(0).unwrap();
            let result = list_array
                .iter()
                .map(|list_element| join_arrow_list_of_utf8s(list_element, delimiter_str));
            Ok(Utf8Array::from((
                self.name(),
                Box::new(arrow2::array::Utf8Array::from_iter(result)),
            )))
        } else {
            assert_eq!(delimiter.len(), self.len());
            let result = list_array.iter().zip(delimiter.as_arrow().iter()).map(
                |(list_element, delimiter_element)| {
                    let delimiter_str = delimiter_element.unwrap_or("");
                    join_arrow_list_of_utf8s(list_element, delimiter_str)
                },
            );
            Ok(Utf8Array::from((
                self.name(),
                Box::new(arrow2::array::Utf8Array::from_iter(result)),
            )))
        }
    }
}
