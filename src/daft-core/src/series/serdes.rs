use std::{borrow::Cow, sync::Arc};

use arrow2::offset::OffsetsBuffer;
use serde::{de::Visitor, Deserializer};

use crate::{
    array::{
        ops::{as_arrow::AsArrow, full::FullNull},
        ListArray, StructArray,
    },
    datatypes::logical::{
        DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
        FixedShapeTensorArray, ImageArray, MapArray, TensorArray, TimeArray, TimestampArray,
    },
    with_match_daft_types, DataType, IntoSeries, Series,
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
                            return Err(serde::de::Error::unknown_field(unknown, EXPECTED_KEYS))
                        }
                    }
                }
                if !values_set {
                    return Err(serde::de::Error::missing_field("values"));
                }
                let field = field.ok_or_else(|| serde::de::Error::missing_field("name"))?;
                use crate::datatypes::*;
                use DataType::*;
                match &field.dtype {
                    Null => Ok(NullArray::full_null(
                        &field.name,
                        &field.dtype,
                        map.next_value::<usize>()?,
                    )
                    .into_series()),
                    Boolean => Ok(BooleanArray::from((
                        field.name.as_str(),
                        map.next_value::<Vec<Option<bool>>>()?.as_slice(),
                    ))
                    .into_series()),
                    Int8 => Ok(Int8Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<i8>>>()?.into_iter(),
                    )
                    .into_series()),
                    Int16 => Ok(Int16Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<i16>>>()?.into_iter(),
                    )
                    .into_series()),
                    Int32 => Ok(Int32Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<i32>>>()?.into_iter(),
                    )
                    .into_series()),
                    Int64 => Ok(Int64Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<i64>>>()?.into_iter(),
                    )
                    .into_series()),
                    Int128 => Ok(Int128Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<i128>>>()?.into_iter(),
                    )
                    .into_series()),
                    UInt8 => Ok(UInt8Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<u8>>>()?.into_iter(),
                    )
                    .into_series()),
                    UInt16 => Ok(UInt16Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<u16>>>()?.into_iter(),
                    )
                    .into_series()),
                    UInt32 => Ok(UInt32Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<u32>>>()?.into_iter(),
                    )
                    .into_series()),
                    UInt64 => Ok(UInt64Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<u64>>>()?.into_iter(),
                    )
                    .into_series()),
                    Float32 => Ok(Float32Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<f32>>>()?.into_iter(),
                    )
                    .into_series()),
                    Float64 => Ok(Float64Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<f64>>>()?.into_iter(),
                    )
                    .into_series()),
                    Utf8 => Ok(Utf8Array::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<str>>>>()?.into_iter(),
                    )
                    .into_series()),
                    Binary => Ok(BinaryArray::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<[u8]>>>>()?.into_iter(),
                    )
                    .into_series()),
                    FixedSizeBinary(size) => Ok(FixedSizeBinaryArray::from_iter(
                        field.name.as_str(),
                        map.next_value::<Vec<Option<Cow<[u8]>>>>()?.into_iter(),
                        *size,
                    )
                    .into_series()),
                    Extension(..) => {
                        let physical = map.next_value::<Series>()?;
                        let physical = physical.to_arrow();
                        let ext_array = physical.to_type(field.dtype.to_arrow().unwrap());
                        Ok(ExtensionArray::new(Arc::new(field), ext_array)
                            .unwrap()
                            .into_series())
                    }
                    Map(..) => {
                        let physical = map.next_value::<Series>()?;
                        Ok(MapArray::new(
                            Arc::new(field),
                            physical.downcast::<ListArray>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    Struct(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let children = all_series
                            .into_iter()
                            .map(|s| s.unwrap())
                            .collect::<Vec<_>>();

                        let validity = validity.map(|v| v.bool().unwrap().as_bitmap().clone());
                        Ok(StructArray::new(Arc::new(field), children, validity).into_series())
                    }
                    List(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let validity = validity.map(|v| v.bool().unwrap().as_bitmap().clone());
                        let offsets_series = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("offsets"))?
                            .unwrap();
                        let offsets_array = offsets_series.i64().unwrap();
                        let offsets = OffsetsBuffer::<i64>::try_from(
                            offsets_array.as_arrow().values().clone(),
                        )
                        .unwrap();
                        let flat_child = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("flat_child"))?
                            .unwrap();
                        Ok(ListArray::new(field, flat_child, offsets, validity).into_series())
                    }
                    FixedSizeList(..) => {
                        let mut all_series = map.next_value::<Vec<Option<Series>>>()?;
                        let validity = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("validity"))?;
                        let flat_child = all_series
                            .pop()
                            .ok_or_else(|| serde::de::Error::missing_field("flat_child"))?
                            .unwrap();

                        let validity = validity.map(|v| v.bool().unwrap().as_bitmap().clone());
                        Ok(FixedSizeListArray::new(field, flat_child, validity).into_series())
                    }
                    Decimal128(..) => {
                        type PType = <<Decimal128Type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(Decimal128Array::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    Timestamp(..) => {
                        type PType = <<TimestampType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(TimestampArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    Date => {
                        type PType = <<DateType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            DateArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    Time(..) => {
                        type PType = <<TimeType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            TimeArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    Duration(..) => {
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
                    Embedding(..) => {
                        type PType = <<EmbeddingType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(EmbeddingArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    Image(..) => {
                        type PType = <<ImageType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            ImageArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    FixedShapeImage(..) => {
                        type PType = <<FixedShapeImageType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(FixedShapeImageArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    FixedShapeTensor(..) => {
                        type PType = <<FixedShapeTensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(FixedShapeTensorArray::new(
                            field,
                            physical.downcast::<PType>().unwrap().clone(),
                        )
                        .into_series())
                    }
                    Tensor(..) => {
                        type PType = <<TensorType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType;
                        let physical = map.next_value::<Series>()?;
                        Ok(
                            TensorArray::new(field, physical.downcast::<PType>().unwrap().clone())
                                .into_series(),
                        )
                    }
                    Python => {
                        panic!("python deserialization not implemented for rust Serde");
                    }
                    Unknown => {
                        panic!("Unable to deserialize Unknown DataType");
                    }
                }
            }
        }
        deserializer.deserialize_map(SeriesVisitor)
    }
}
