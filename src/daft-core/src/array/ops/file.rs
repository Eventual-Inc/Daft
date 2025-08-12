use common_file::DaftFileType;
use daft_schema::{dtype::DataType, field::Field};

use crate::{
    array::StructArray,
    datatypes::logical::FileArray,
    prelude::{BinaryArray, UInt8Array, Utf8Array},
    series::IntoSeries,
};

impl FileArray {
    pub fn new_from_reference_array(name: &str, values: &Utf8Array) -> Self {
        let discriminant_field = Field::new("discriminant", DataType::UInt8);
        let values_field = Field::new("data", DataType::Binary);

        let discriminant_values = vec![DaftFileType::Reference as u8; values.len()];

        let discriminant = UInt8Array::from_values_iter(
            discriminant_field.clone(),
            discriminant_values.into_iter(),
        )
        .into_series();
        let fld = Field::new(
            "literal",
            DataType::Struct(vec![discriminant_field, values_field]),
        );
        let sa = StructArray::new(
            fld,
            vec![
                discriminant,
                values
                    .clone()
                    .into_series()
                    .cast(&DataType::Binary)
                    .unwrap()
                    .rename("data"),
            ],
            values.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File), sa)
    }

    pub fn new_from_data_array(name: &str, values: &BinaryArray) -> Self {
        let discriminant_field = Field::new("discriminant", DataType::UInt8);
        let values_field = Field::new("data", DataType::Binary);

        let discriminant_values = vec![DaftFileType::Data as u8; values.len()];

        let discriminant = UInt8Array::from_values_iter(
            discriminant_field.clone(),
            discriminant_values.into_iter(),
        )
        .into_series();
        let fld = Field::new(
            "literal",
            DataType::Struct(vec![discriminant_field, values_field]),
        );
        let sa = StructArray::new(
            fld,
            vec![discriminant, values.clone().into_series().rename("data")],
            values.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File), sa)
    }

    pub fn discriminant_array(&self) -> UInt8Array {
        self.physical
            .get("discriminant")
            .unwrap()
            .u8()
            .unwrap()
            .clone()
    }

    pub fn data_array(&self) -> BinaryArray {
        self.physical.get("data").unwrap().binary().unwrap().clone()
    }
}
