use std::{collections::HashMap, hash::Hash, sync::Arc};

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use common_error::{DaftError, DaftResult};
use daft_arrow::datatypes::Field as ArrowField;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::dtype::{DAFT_SUPER_EXTENSION_NAME, DataType};

pub type Metadata = std::collections::BTreeMap<String, String>;

#[derive(Clone, Display, Debug, Eq, Deserialize, Serialize)]
#[display("{name}#{dtype}")]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
    pub metadata: Arc<Metadata>,
}

pub type FieldRef = Arc<Field>;
pub type DaftField = Field;

#[derive(Clone, Display, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[display("{id}")]
pub struct FieldID {
    pub id: Arc<str>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        // Skips metadata check
        self.name == other.name && self.dtype == other.dtype
    }
}

impl Hash for Field {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.dtype.hash(state);
        // Skip hashing metadata
    }
}

impl FieldID {
    pub fn new<S: Into<Arc<str>>>(id: S) -> Self {
        Self { id: id.into() }
    }

    /// Create a Field ID directly from a real column name.
    /// Performs sanitization on the name so it can be composed.
    pub fn from_name<S: Into<String>>(name: S) -> Self {
        let name: String = name.into();

        // Escape parentheses within a string,
        // since we will use parentheses as delimiters in our semantic expression IDs.
        let sanitized = name
            .replace('.', "\\x2e")
            .replace(',', "\\x2c")
            .replace('(', "\\x28")
            .replace(')', "\\x29")
            .replace('\\', "\\x5c");
        Self::new(sanitized)
    }
}

impl Field {
    pub fn new<S: Into<String>>(name: S, dtype: DataType) -> Self {
        let name: String = name.into();
        Self {
            name,
            dtype,
            metadata: Default::default(),
        }
    }

    pub fn with_metadata<M: Into<Arc<Metadata>>>(self, metadata: M) -> Self {
        Self {
            name: self.name,
            dtype: self.dtype,
            metadata: metadata.into(),
        }
    }

    #[deprecated(note = "use .to_arrow")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn to_arrow2(&self) -> DaftResult<ArrowField> {
        Ok(
            ArrowField::new(self.name.clone(), self.dtype.to_arrow2()?, true)
                .with_metadata(self.metadata.as_ref().clone()),
        )
    }

    pub fn to_arrow(&self) -> DaftResult<arrow_schema::Field> {
        fn dtype_to_arrow(dtype: &DataType) -> DaftResult<arrow_schema::DataType> {
            let dtype = match dtype {
                DataType::Null => arrow_schema::DataType::Null,
                DataType::Boolean => arrow_schema::DataType::Boolean,
                DataType::Int8 => arrow_schema::DataType::Int8,
                DataType::Int16 => arrow_schema::DataType::Int16,
                DataType::Int32 => arrow_schema::DataType::Int32,
                DataType::Int64 => arrow_schema::DataType::Int64,
                DataType::UInt8 => arrow_schema::DataType::UInt8,
                DataType::UInt16 => arrow_schema::DataType::UInt16,
                DataType::UInt32 => arrow_schema::DataType::UInt32,
                DataType::UInt64 => arrow_schema::DataType::UInt64,
                DataType::Float32 => arrow_schema::DataType::Float32,
                DataType::Float64 => arrow_schema::DataType::Float64,
                DataType::Timestamp(unit, tz) => {
                    arrow_schema::DataType::Timestamp(unit.to_arrow(), tz.clone().map(Arc::from))
                }
                DataType::Duration(unit) => arrow_schema::DataType::Duration(unit.to_arrow()),
                DataType::Interval => {
                    arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
                }
                DataType::Binary => arrow_schema::DataType::LargeBinary,
                DataType::FixedSizeBinary(size) => {
                    arrow_schema::DataType::FixedSizeBinary(*size as _)
                }
                DataType::Utf8 => arrow_schema::DataType::LargeUtf8,
                DataType::List(f) => {
                    let inner_field = Field::new("item", f.as_ref().clone());
                    let arrow_field = Arc::new(inner_field.to_arrow()?);
                    arrow_schema::DataType::LargeList(arrow_field)
                }
                DataType::FixedSizeList(f, size) => {
                    let inner_field = Field::new("item", f.as_ref().clone());
                    arrow_schema::DataType::FixedSizeList(
                        Arc::new(inner_field.to_arrow()?),
                        *size as _,
                    )
                }
                DataType::Struct(fields) => arrow_schema::DataType::Struct(
                    fields
                        .iter()
                        .map(|f| f.to_arrow())
                        .collect::<DaftResult<Vec<_>>>()?
                        .into(),
                ),
                DataType::Map { key, value } => {
                    let key_field = Field::new("key", key.as_ref().clone());
                    let value_field = Field::new("value", value.as_ref().clone());

                    let struct_type = arrow_schema::DataType::Struct(
                        vec![
                            key_field.to_arrow()?.with_nullable(false),
                            value_field.to_arrow()?,
                        ]
                        .into(),
                    );
                    let struct_field = arrow_schema::Field::new("entries", struct_type, false);

                    arrow_schema::DataType::Map(Arc::new(struct_field), false)
                }
                DataType::Decimal128(precision, scale) => {
                    arrow_schema::DataType::Decimal128(*precision as _, *scale as _)
                }
                DataType::Date => arrow_schema::DataType::Date32,
                DataType::Time(time_unit) => arrow_schema::DataType::Time64(time_unit.to_arrow()),

                _ => {
                    return Err(DaftError::TypeError(format!(
                        "Can not convert {dtype:?} into arrow type"
                    )));
                }
            };
            Ok(dtype)
        }

        let field = match &self.dtype {
            DataType::Extension(name, dtype, metadata) => {
                let physical =
                    arrow_schema::Field::new(self.name.clone(), dtype_to_arrow(dtype)?, true);
                let mut metadata_map = HashMap::new();
                metadata_map.insert(EXTENSION_TYPE_NAME_KEY.to_string(), name.clone());
                if let Some(metadata) = metadata {
                    metadata_map.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), metadata.clone());
                }
                physical.with_metadata(metadata_map)
            }
            dtype @ DataType::Embedding(..)
            | dtype @ DataType::Image(..)
            | dtype @ DataType::FixedShapeImage(..)
            | dtype @ DataType::Tensor(..)
            | dtype @ DataType::FixedShapeTensor(..)
            | dtype @ DataType::SparseTensor(..)
            | dtype @ DataType::FixedShapeSparseTensor(..)
            | dtype @ DataType::File(..) => {
                let physical = Box::new(self.to_physical());

                let mut metadata_map = HashMap::new();
                metadata_map.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    DAFT_SUPER_EXTENSION_NAME.into(),
                );

                metadata_map.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), dtype.to_json()?);

                physical.to_arrow()?.with_metadata(metadata_map)
            }
            #[cfg(feature = "python")]
            dtype @ DataType::Python => {
                let mut physical = self.clone();
                physical.dtype = DataType::Binary;
                let physical = Box::new(physical);

                let mut metadata_map = HashMap::new();
                metadata_map.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    DAFT_SUPER_EXTENSION_NAME.into(),
                );

                metadata_map.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), dtype.to_json()?);

                physical.to_arrow()?.with_metadata(metadata_map)
            }
            _ => arrow_schema::Field::new(self.name.clone(), dtype_to_arrow(&self.dtype)?, true),
        };

        let meta = field.metadata().clone();
        Ok(field.with_metadata(
            self.metadata
                .as_ref()
                .clone()
                .into_iter()
                .chain(meta)
                .collect(),
        ))
    }

    pub fn to_physical(&self) -> Self {
        Self {
            name: self.name.clone(),
            dtype: self.dtype.to_physical(),
            metadata: self.metadata.clone(),
        }
    }

    pub fn rename<S: Into<String>>(&self, name: S) -> Self {
        Self {
            name: name.into(),
            dtype: self.dtype.clone(),
            metadata: self.metadata.clone(),
        }
    }

    pub fn to_list_field(&self) -> Self {
        let list_dtype = DataType::List(Box::new(self.dtype.clone()));
        Self {
            name: self.name.clone(),
            dtype: list_dtype,
            metadata: self.metadata.clone(),
        }
    }

    pub fn to_fixed_size_list_field(&self, size: usize) -> DaftResult<Self> {
        let list_dtype = DataType::FixedSizeList(Box::new(self.dtype.clone()), size);
        Ok(Self {
            name: self.name.clone(),
            dtype: list_dtype,
            metadata: self.metadata.clone(),
        })
    }

    pub fn to_exploded_field(&self) -> DaftResult<Self> {
        match &self.dtype {
            DataType::List(child_dtype) | DataType::FixedSizeList(child_dtype, _) => Ok(Self {
                name: self.name.clone(),
                dtype: child_dtype.as_ref().clone(),
                metadata: self.metadata.clone(),
            }),
            _ => Err(DaftError::ValueError(format!(
                "Column \"{}\" with dtype {} cannot be exploded, must be a List or FixedSizeList column.",
                self.name, self.dtype,
            ))),
        }
    }
}

impl From<&ArrowField> for Field {
    fn from(af: &ArrowField) -> Self {
        Self {
            name: af.name.clone(),
            dtype: af.data_type().into(),
            metadata: af.metadata.clone().into(),
        }
    }
}

impl TryFrom<&arrow_schema::Field> for Field {
    type Error = DaftError;

    fn try_from(value: &arrow_schema::Field) -> Result<Self, Self::Error> {
        if value.extension_type_name() == Some(DAFT_SUPER_EXTENSION_NAME) {
            let metadata = value.extension_type_metadata()
                .expect("DataType::try_from<&arrow_schema::Field> failed to get metadata for extension type");
            let dtype = DataType::from_json(metadata)?;

            let mut metadata = value.metadata().clone();
            metadata.remove(EXTENSION_TYPE_NAME_KEY);
            metadata.remove(EXTENSION_TYPE_METADATA_KEY);

            Ok(Self {
                name: value.name().clone(),
                dtype,
                metadata: Arc::new(metadata.into_iter().collect()),
            })
        } else {
            Ok(Self {
                name: value.name().clone(),
                dtype: value.try_into()?,
                metadata: Arc::new(value.metadata().clone().into_iter().collect()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use rstest::rstest;

    use crate::{
        dtype::{DAFT_SUPER_EXTENSION_NAME, DataType},
        field::Field,
        media_type::MediaType,
        prelude::{ImageMode, TimeUnit},
    };

    #[test]
    fn test_field_with_extension_type_to_arrow() -> DaftResult<()> {
        let field = Field::new(
            "embeddings",
            DataType::Embedding(Box::new(DataType::Float64), 512),
        );
        let arrow_field = field.to_arrow()?;
        assert_eq!(arrow_field.name(), "embeddings");
        assert_eq!(arrow_field.metadata().len(), 2);
        assert_eq!(
            arrow_field.extension_type_name(),
            Some(DAFT_SUPER_EXTENSION_NAME)
        );
        assert_eq!(
            arrow_field.extension_type_metadata(),
            Some(field.dtype.to_json()?.as_str())
        );

        Ok(())
    }

    #[rstest]
    #[case(DataType::Embedding(Box::new(DataType::Float64), 512))]
    #[case(DataType::Embedding(Box::new(DataType::Float32), 512))]
    #[case(DataType::Image(None))]
    #[case(DataType::Image(Some(ImageMode::L)))]
    #[case(DataType::Image(Some(ImageMode::RGB)))]
    #[case(DataType::Image(Some(ImageMode::RGBA)))]
    #[case(DataType::Image(Some(ImageMode::L16)))]
    #[case(DataType::Image(Some(ImageMode::LA16)))]
    #[case(DataType::Image(Some(ImageMode::RGB16)))]
    #[case(DataType::Image(Some(ImageMode::RGBA16)))]
    #[case(DataType::FixedShapeImage(ImageMode::RGB, 256, 256))]
    #[case(DataType::Tensor(Box::new(DataType::Float32)))]
    #[case(DataType::FixedShapeTensor(Box::new(DataType::Float32), vec![3, 224, 224]))]
    #[case(DataType::SparseTensor(Box::new(DataType::Float64), true))]
    #[case(DataType::FixedShapeSparseTensor(Box::new(DataType::Float64), vec![100, 100], true))]
    #[case(DataType::File(MediaType::Video))]
    #[case(DataType::File(MediaType::Unknown))]
    #[case(DataType::File(MediaType::Audio))]
    #[case(DataType::List(Box::new(DataType::Image(Some(ImageMode::RGBA16)))))]
    #[case(DataType::List(Box::new(DataType::Embedding(Box::new(DataType::Float64), 128))))]
    #[case(DataType::List(Box::new(DataType::Tensor(Box::new(DataType::Int32)))))]
    #[case(DataType::FixedSizeList(Box::new(DataType::Image(Some(ImageMode::RGBA16))), 10))]
    #[case(DataType::FixedSizeList(
        Box::new(DataType::Embedding(Box::new(DataType::Float32), 256)),
        5
    ))]
    #[case(DataType::List(Box::new(DataType::List(Box::new(DataType::Image(Some(
        ImageMode::RGB
    )))))))]
    #[case(DataType::Map {
        key: Box::new(DataType::Utf8),
        value: Box::new(DataType::Image(Some(ImageMode::RGBA)))
    })]
    #[case(DataType::Map {
        key: Box::new(DataType::Utf8),
        value: Box::new(DataType::Embedding(Box::new(DataType::Float64), 512))
    })]
    #[case(DataType::Struct(vec![
        Field::new("embedding", DataType::Embedding(Box::new(DataType::Float64), 512)),
        Field::new("id", DataType::UInt32),
        Field::new("date", DataType::Date),
        Field::new("ts", DataType::Timestamp(TimeUnit::Milliseconds, None)),
        Field::new("file", DataType::File(MediaType::Video)),
        Field::new("name", DataType::Struct(vec![
            Field::new("first_name", DataType::Utf8),
            Field::new("last_name", DataType::Utf8),
        ])),
        Field::new("list_of_images", DataType::FixedSizeList(Box::new(DataType::Image(Some(ImageMode::RGBA16))), 1))
    ]))]
    #[case(DataType::Struct(vec![
        Field::new("nested_struct", DataType::Struct(vec![
            Field::new("tensor", DataType::Tensor(Box::new(DataType::Float32))),
            Field::new("image", DataType::Image(None)),
        ])),
        Field::new("list_of_tensors", DataType::List(Box::new(DataType::FixedShapeTensor(Box::new(DataType::Int64), vec![10, 10])))),
    ]))]
    #[case(DataType::Extension("custom_ext".to_string(), Box::new(DataType::Binary), Some("metadata".to_string())))]
    #[case(DataType::Extension("custom_ext".to_string(), Box::new(DataType::Binary), None))]
    #[case(DataType::List(Box::new(DataType::Extension("nested_ext".to_string(), Box::new(DataType::Int32), None))))]
    fn test_field_arrow_round_trip(#[case] dtype: DataType) -> DaftResult<()> {
        let field = Field::new("test", dtype.clone());
        let arrow_field = field.to_arrow()?;
        let round_trip_dtype = DataType::try_from(&arrow_field)?;
        assert_eq!(dtype, round_trip_dtype);

        Ok(())
    }
}
