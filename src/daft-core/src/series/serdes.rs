use std::{borrow::Cow, sync::Arc};

use daft_arrow::{offset::OffsetsBuffer, types::months_days_ns};
use serde::{Deserializer, de::Visitor};

use crate::{
    array::{
        ListArray, StructArray,
        ops::{as_arrow::AsArrow, full::FullNull},
    },
    datatypes::{
        logical::{
            DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeSparseTensorArray, FixedShapeTensorArray, ImageArray, MapArray,
            SparseTensorArray, TensorArray, TimeArray, TimestampArray,
        },
        *,
    },
    series::{IntoSeries, Series},
    with_match_daft_types,
};

impl serde::Serialize for Series {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        with_match_daft_types!(self.data_type(), |$T| {
            let array = self.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            array.serialize(serializer)
        })
    }
}

impl<'d> serde::Deserialize<'d> for Series {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        const EXPECTED_KEYS: &[&str] = &["field", "values"];

        struct SeriesVisitor;

        impl<'d> Visitor<'d> for SeriesVisitor {
            type Value = Series;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct {field: <field>, values: <values array>}")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'d>,
            {
                let mut field: Option<Field> = None;
                let mut values_set = false;
                while let Some(key) = map.next_key::<Cow<str>>()? {
                    match key.as_ref() {
                        "field" => {
                            field = Some(map.next_value()?);
                        }
                        "values" => {
                            values_set = true;
                            break;
                        }
                        unknown => {
                            return Err(serde::de::Error::unknown_field(unknown, EXPECTED_KEYS));
                        }
                    }
                }
                if !values_set {
                    return Err(serde::de::Error::missing_field("values"));
                }
                let field = field.ok_or_else(|| serde::de::Error::missing_field("name"))?;
                match &field.dtype {
                    DataType::Null => Ok(NullArray::full_null(
                        &field.name,
                        &field.dtype,
                        map.next_value::<usize>()?,
                    )
                    .into_series()),
                    DataType::Boolean => Ok(BooleanArray::from((
                        field.name.as_str(),
                        map.next_value::<Vec<Option<bool>>>()?.as_slice(),
                    ))
                    .into_series()),
                    DataType::Int8 => Ok(Int8Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<i8>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Int16 => Ok(Int16Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<i16>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Int32 => Ok(Int32Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<i32>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Int64 => Ok(Int64Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<i64>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::UInt8 => Ok(UInt8Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<u8>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::UInt16 => Ok(UInt16Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<u16>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::UInt32 => Ok(UInt32Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<u32>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::UInt64 => Ok(UInt64Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<u64>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Float32 => Ok(Float32Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<f32>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Float64 => Ok(Float64Array::from_iter(
                        field,
                        map.next_value::<Vec<Option<f64>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Utf8 => Ok(Utf8Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<str>>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Binary => Ok(BinaryArray::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<[u8]>>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::FixedSizeBinary(size) => Ok(FixedSizeBinaryArray::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<[u8]>>>>()?.into_iter(),
                        *size,
                    )
                    .into_series()),
                    DataType::Extension(..) => {
                        let physical = map.next_value::<Series>()?;
                        let physical = physical.to_arrow2();
                        let ext_array =
                            physical.convert_logical_type(field.dtype.to_arrow().unwrap());
                        Ok(ExtensionArray::new(Arc::new(field), ext_array)
                            .unwrap()
                            .into_series())
                    }
                    DataType::Map { .. } => {
                        let physical = map.next_value::<Series>()?;
                        Ok(MapArray::new(
                            Arc::new(field),
                            physical.downcast::<ListArray>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::Struct(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let children = all_series
                            .into_iter()
                            .map(|s| s.unwrap())
                            .collect::<Vec<_>>();

                        let validity =
                            validity.map(|v| v.bool().unwrap().as_bitmap().clone().into());
                        Ok(StructArray::new(Arc::new(field), children, validity).into_series())
                    }
                    DataType::List(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let validity =
                            validity.map(|v| v.bool().unwrap().as_bitmap().clone().into());
                        let offsets_series = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("offsets"))?
                            .unwrap();
                        let offsets_array = offsets_series.i64().unwrap();
                        let offsets = OffsetsBuffer::<i64>::try_from(
                            offsets_array.as_arrow2().values().clone(),
                        )
                        .unwrap();
                        let flat_child = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("flat_child"))?
                            .unwrap();
                        Ok(ListArray::new(field, flat_child, offsets, validity).into_series())
                    }
                    DataType::FixedSizeList(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let flat_child = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("flat_child"))?
                            .unwrap();

                        let validity =
                            validity.map(|v| v.bool().unwrap().as_bitmap().clone().into());
                        Ok(FixedSizeListArray::new(field, flat_child, validity).into_series())
                    }
                    DataType::Decimal128(..) => Ok(Decimal128Array::from_iter(
                        Arc::new(field.clone()),
                        map.next_value::<Vec<Option<i128>>>()?.into_iter(),
                    )
                    .into_series()),
                    DataType::Timestamp(..) => {
                        type PType = <<TimestampType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(TimestampArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::Date => {
                        type PType = <<DateType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            DateArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    DataType::Time(..) => {
                        type PType = <<TimeType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            TimeArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    DataType::Duration(..) => {
                        type PType = <<DurationType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;

                        Ok(
                            DurationArray::new(
                                field,
                                physical.downcast::<PType>().unwrap().clone(),
                            )
                            .into_series(),
                        )
                    }
                    DataType::Interval => Ok(IntervalArray::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<months_days_ns>>>()?.into_iter(),
                    )
                    .into_series()),

                    DataType::Embedding(..) => {
                        type PType = <<EmbeddingType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(EmbeddingArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::Image(..) => {
                        type PType = <<ImageType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            ImageArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    DataType::FixedShapeImage(..) => {
                        type PType = <<FixedShapeImageType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(FixedShapeImageArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::FixedShapeTensor(..) => {
                        type PType = <<FixedShapeTensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(FixedShapeTensorArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::SparseTensor(..) => {
                        type PType = <<SparseTensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(SparseTensorArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::FixedShapeSparseTensor(..) => {
                        type PType = <<FixedShapeSparseTensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(FixedShapeSparseTensorArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    DataType::Tensor(..) => {
                        type PType = <<TensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            TensorArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    #[cfg(feature = "python")]
                    DataType::Python => {
                        use crate::prelude::PythonArray;

                        let pickled = map.next_value::<Vec<Option<Cow<[u8]>>>>()?.into_iter();
                        Ok(PythonArray::from_iter_pickled(&field.name, pickled)
                            .unwrap()
                            .into_series())
                    }
                    DataType::Unknown => {
                        panic!("Unable to deserialize Unknown DataType");
                    }
                    DataType::File(_) => {
                        panic!("Unable to deserialize File DataType");
                    }
                }
            }
        }
        deserializer.deserialize_map(SeriesVisitor)
    }
}
