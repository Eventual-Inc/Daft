use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, LazyLock, Mutex},
};

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use common_error::{DaftError, DaftResult};
use daft_arrow::datatypes::Field as ArrowField;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::dtype::{DAFT_SUPER_EXTENSION_NAME, DataType};

const ARROW_FIXED_SHAPE_TENSOR_KEY: &str = "arrow.fixed_shape_tensor";
const ARROW_VARIABLE_SHAPE_TENSOR_KEY: &str = "arrow.variable_shape_tensor";
/// Registry that maps extension type names to their original arrow-rs storage DataType
/// (before Daft coercion, e.g. Binary instead of LargeBinary). This allows `to_arrow`
/// to reverse the coercion so PyArrow sees the original storage type.
static EXTENSION_TYPE_REGISTRY: LazyLock<Mutex<HashMap<String, arrow_schema::DataType>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
    pub fn to_arrow2(&self) -> DaftResult<ArrowField> {
        Ok(
            ArrowField::new(self.name.clone(), self.dtype.to_arrow2()?, true)
                .with_metadata(self.metadata.as_ref().clone()),
        )
    }
    pub fn to_arrow(&self) -> DaftResult<arrow_schema::Field> {
        let field = match &self.dtype {
            DataType::Extension(name, dtype, metadata) => {
                // If we previously registered the original (pre-coercion) storage type,
                // use it so PyArrow sees the original type (e.g. Binary, not LargeBinary).
                let storage_type = EXTENSION_TYPE_REGISTRY
                    .lock()
                    .unwrap()
                    .get(name)
                    .cloned()
                    .map_or_else(|| dtype.to_arrow(), Ok)?;

                let physical = arrow_schema::Field::new(self.name.clone(), storage_type, true);
                let mut metadata_map = HashMap::new();
                metadata_map.insert(EXTENSION_TYPE_NAME_KEY.to_string(), name.clone());
                if let Some(metadata) = metadata {
                    metadata_map.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), metadata.clone());
                }
                physical.with_metadata(metadata_map)
            }
            DataType::Tensor(..) => {
                let physical = Box::new(self.to_physical());
                let mut metadata_map = HashMap::new();
                metadata_map.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    ARROW_VARIABLE_SHAPE_TENSOR_KEY.into(),
                );

                physical.to_arrow()?.with_metadata(metadata_map)
            }
            DataType::FixedShapeTensor(_, shape) => {
                let physical = Box::new(self.to_physical());
                let mut metadata_map = HashMap::new();
                metadata_map.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    ARROW_FIXED_SHAPE_TENSOR_KEY.into(),
                );

                let shape_json = serde_json::json!({ "shape": shape }).to_string();
                metadata_map.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), shape_json);

                physical.to_arrow()?.with_metadata(metadata_map)
            }

            dtype @ DataType::Embedding(..)
            | dtype @ DataType::Image(..)
            | dtype @ DataType::FixedShapeImage(..)
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
            _ => arrow_schema::Field::new(self.name.clone(), self.dtype.to_arrow()?, true),
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

    fn try_from(field: &arrow_schema::Field) -> Result<Self, Self::Error> {
        if field.extension_type_name() == Some(DAFT_SUPER_EXTENSION_NAME) {
            let metadata = field.extension_type_metadata()
                         .expect("DataType::try_from<&arrow_schema::Field> failed to get metadata for extension type");
            let dtype = DataType::from_json(metadata)?;

            let mut metadata = field.metadata().clone();
            metadata.remove(EXTENSION_TYPE_NAME_KEY);
            metadata.remove(EXTENSION_TYPE_METADATA_KEY);

            Ok(Self {
                name: field.name().clone(),
                dtype,
                metadata: Arc::new(metadata.into_iter().collect()),
            })
        } else if field.extension_type_name() == Some(ARROW_FIXED_SHAPE_TENSOR_KEY) {
            arrow_fixed_shape_tensor_to_daft(field)
        } else if field.extension_type_name() == Some(ARROW_VARIABLE_SHAPE_TENSOR_KEY) {
            arrow_variable_shape_tensor_to_daft(field)
        } else if let Some(extension_name) = field.extension_type_name() {
            // Generic extension type (e.g. daft.uuid)
            let physical = DataType::try_from(field.data_type())?;
            let ext_metadata = field.extension_type_metadata().map(|s| s.to_string());

            // Remember the original arrow storage type so to_arrow() can
            // reverse the coercion (e.g. Binary instead of LargeBinary).
            EXTENSION_TYPE_REGISTRY
                .lock()
                .unwrap()
                .insert(extension_name.to_string(), field.data_type().clone());

            let mut field_metadata = field.metadata().clone();
            field_metadata.remove(EXTENSION_TYPE_NAME_KEY);
            field_metadata.remove(EXTENSION_TYPE_METADATA_KEY);

            Ok(Self {
                name: field.name().clone(),
                dtype: DataType::Extension(
                    extension_name.to_string(),
                    Box::new(physical),
                    ext_metadata,
                ),
                metadata: Arc::new(field_metadata.into_iter().collect()),
            })
        } else {
            Ok(Self {
                name: field.name().clone(),
                dtype: field.try_into()?,
                metadata: Arc::new(field.metadata().clone().into_iter().collect()),
            })
        }
    }
}

fn arrow_fixed_shape_tensor_to_daft(field: &arrow_schema::Field) -> Result<Field, DaftError> {
    // Arrow canonical fixed_shape_tensor:
    //   storage = FixedSizeList<value_type>(product_of_shape)
    //   metadata JSON = {"shape": [d0, d1, ...], ...}
    let metadata = field
        .extension_type_metadata()
        .ok_or_else(|| DaftError::TypeError("arrow.fixed_shape_tensor missing metadata".into()))?;
    let json: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|e| DaftError::TypeError(format!("invalid fixed_shape_tensor metadata: {e}")))?;
    let shape: Vec<u64> = json
        .get("shape")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            DaftError::TypeError("fixed_shape_tensor metadata missing 'shape' array".into())
        })?
        .iter()
        .map(|v| {
            v.as_u64()
                .ok_or_else(|| DaftError::TypeError("shape element is not a u64".into()))
        })
        .collect::<DaftResult<_>>()?;

    let value_type = match field.data_type() {
        arrow_schema::DataType::FixedSizeList(inner, _) => DataType::try_from(inner.data_type())?,
        other => {
            return Err(DaftError::TypeError(format!(
                "expected FixedSizeList storage for arrow.fixed_shape_tensor, got {other:?}"
            )));
        }
    };

    let mut field_metadata = field.metadata().clone();
    field_metadata.remove(EXTENSION_TYPE_NAME_KEY);
    field_metadata.remove(EXTENSION_TYPE_METADATA_KEY);

    Ok(Field {
        name: field.name().clone(),
        dtype: DataType::FixedShapeTensor(Box::new(value_type), shape),
        metadata: Arc::new(field_metadata.into_iter().collect()),
    })
}

fn arrow_variable_shape_tensor_to_daft(field: &arrow_schema::Field) -> Result<Field, DaftError> {
    // Arrow canonical variable_shape_tensor:
    //   storage = Struct { data: List<value_type>, shape: FixedSizeList<Int32>(ndim) }
    let value_type = match field.data_type() {
        arrow_schema::DataType::Struct(fields) => {
            let data_field = fields.iter().find(|f| f.name() == "data").ok_or_else(|| {
                DaftError::TypeError("variable_shape_tensor struct missing 'data' field".into())
            })?;
            match data_field.data_type() {
                arrow_schema::DataType::List(inner) | arrow_schema::DataType::LargeList(inner) => {
                    DataType::try_from(inner.data_type())?
                }
                other => {
                    return Err(DaftError::TypeError(format!(
                        "expected List for variable_shape_tensor 'data' field, got {other:?}"
                    )));
                }
            }
        }
        other => {
            return Err(DaftError::TypeError(format!(
                "expected Struct storage for arrow.variable_shape_tensor, got {other:?}"
            )));
        }
    };

    let mut field_metadata = field.metadata().clone();
    field_metadata.remove(EXTENSION_TYPE_NAME_KEY);
    field_metadata.remove(EXTENSION_TYPE_METADATA_KEY);

    Ok(Field {
        name: field.name().clone(),
        dtype: DataType::Tensor(Box::new(value_type)),
        metadata: Arc::new(field_metadata.into_iter().collect()),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
    use common_error::DaftResult;
    use rstest::rstest;

    use crate::{
        dtype::{DAFT_SUPER_EXTENSION_NAME, DataType},
        field::{ARROW_FIXED_SHAPE_TENSOR_KEY, ARROW_VARIABLE_SHAPE_TENSOR_KEY, Field},
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

    /// Helper to build an arrow_schema::Field with extension type metadata,
    /// exactly as Arrow's canonical extension types are represented.
    fn arrow_ext_field(
        name: &str,
        storage_type: arrow_schema::DataType,
        ext_name: &str,
        ext_metadata: Option<&str>,
    ) -> arrow_schema::Field {
        let mut metadata = HashMap::new();
        metadata.insert(EXTENSION_TYPE_NAME_KEY.to_string(), ext_name.to_string());
        if let Some(m) = ext_metadata {
            metadata.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), m.to_string());
        }
        arrow_schema::Field::new(name, storage_type, true).with_metadata(metadata)
    }

    /// Arrow canonical `arrow.fixed_shape_tensor` with Float32 elements and shape [3, 224, 224].
    /// Storage type: FixedSizeList<Float32>(3 * 224 * 224 = 150528)
    /// Metadata: {"shape": [3, 224, 224]}
    #[test]
    fn test_arrow_canonical_fixed_shape_tensor_to_daft() {
        let storage = arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            )),
            3 * 224 * 224,
        );
        let arrow_field = arrow_ext_field(
            "tensor_col",
            storage,
            ARROW_FIXED_SHAPE_TENSOR_KEY,
            Some(r#"{"shape":[3,224,224]}"#),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(daft_field.name, "tensor_col");
        assert_eq!(
            daft_field.dtype,
            DataType::FixedShapeTensor(Box::new(DataType::Float32), vec![3, 224, 224])
        );
    }

    /// Arrow canonical `arrow.fixed_shape_tensor` with Int64 elements and shape [10, 10].
    #[test]
    fn test_arrow_canonical_fixed_shape_tensor_int64() {
        let storage = arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Int64,
                true,
            )),
            100,
        );
        let arrow_field = arrow_ext_field(
            "tensor",
            storage,
            ARROW_FIXED_SHAPE_TENSOR_KEY,
            Some(r#"{"shape":[10,10]}"#),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::FixedShapeTensor(Box::new(DataType::Int64), vec![10, 10])
        );
    }

    /// Arrow canonical `arrow.fixed_shape_tensor` with extra dim_names/permutation metadata.
    /// Daft should still extract the shape and element type correctly.
    #[test]
    fn test_arrow_canonical_fixed_shape_tensor_with_dim_names() {
        let storage = arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float64,
                true,
            )),
            100 * 200 * 3,
        );
        let arrow_field = arrow_ext_field(
            "image_tensor",
            storage,
            ARROW_FIXED_SHAPE_TENSOR_KEY,
            Some(r#"{"shape":[100,200,3],"dim_names":["H","W","C"]}"#),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::FixedShapeTensor(Box::new(DataType::Float64), vec![100, 200, 3])
        );
    }

    /// Arrow canonical `arrow.fixed_shape_tensor` with a 1-D shape.
    #[test]
    fn test_arrow_canonical_fixed_shape_tensor_1d() {
        let storage = arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            )),
            512,
        );
        let arrow_field = arrow_ext_field(
            "embedding",
            storage,
            ARROW_FIXED_SHAPE_TENSOR_KEY,
            Some(r#"{"shape":[512]}"#),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::FixedShapeTensor(Box::new(DataType::Float32), vec![512])
        );
    }

    /// Arrow canonical `arrow.variable_shape_tensor` with Float32 elements, 3 dimensions.
    /// Storage type: Struct { data: List<Float32>, shape: FixedSizeList<Int32>(3) }
    #[test]
    fn test_arrow_canonical_variable_shape_tensor_to_daft() {
        let data_field = arrow_schema::Field::new(
            "data",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            ))),
            true,
        );
        let shape_field = arrow_schema::Field::new(
            "shape",
            arrow_schema::DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Int32,
                    true,
                )),
                3,
            ),
            true,
        );
        let storage = arrow_schema::DataType::Struct(vec![data_field, shape_field].into());

        // Minimal metadata (empty string is valid per spec)
        let arrow_field = arrow_ext_field(
            "tensor_col",
            storage,
            ARROW_VARIABLE_SHAPE_TENSOR_KEY,
            Some(""),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(daft_field.name, "tensor_col");
        assert_eq!(
            daft_field.dtype,
            DataType::Tensor(Box::new(DataType::Float32))
        );
    }

    /// Arrow canonical `arrow.variable_shape_tensor` with Int32 elements, 2 dimensions.
    #[test]
    fn test_arrow_canonical_variable_shape_tensor_int32() {
        let data_field = arrow_schema::Field::new(
            "data",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Int32,
                true,
            ))),
            true,
        );
        let shape_field = arrow_schema::Field::new(
            "shape",
            arrow_schema::DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Int32,
                    true,
                )),
                2,
            ),
            true,
        );
        let storage = arrow_schema::DataType::Struct(vec![data_field, shape_field].into());

        let arrow_field = arrow_ext_field("matrix", storage, ARROW_VARIABLE_SHAPE_TENSOR_KEY, None);

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::Tensor(Box::new(DataType::Int32))
        );
    }

    /// Arrow canonical `arrow.variable_shape_tensor` with dim_names metadata.
    #[test]
    fn test_arrow_canonical_variable_shape_tensor_with_dim_names() {
        let data_field = arrow_schema::Field::new(
            "data",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float64,
                true,
            ))),
            true,
        );
        let shape_field = arrow_schema::Field::new(
            "shape",
            arrow_schema::DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Int32,
                    true,
                )),
                3,
            ),
            true,
        );
        let storage = arrow_schema::DataType::Struct(vec![data_field, shape_field].into());

        let arrow_field = arrow_ext_field(
            "video_frames",
            storage,
            ARROW_VARIABLE_SHAPE_TENSOR_KEY,
            Some(r#"{"dim_names":["H","W","C"]}"#),
        );

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::Tensor(Box::new(DataType::Float64))
        );
    }

    /// A List of arrow.fixed_shape_tensor should become List<FixedShapeTensor>.
    #[test]
    fn test_arrow_canonical_list_of_fixed_shape_tensors() {
        let inner_storage = arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            )),
            100,
        );
        let mut inner_metadata = HashMap::new();
        inner_metadata.insert(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            ARROW_FIXED_SHAPE_TENSOR_KEY.to_string(),
        );
        inner_metadata.insert(
            EXTENSION_TYPE_METADATA_KEY.to_string(),
            r#"{"shape":[10,10]}"#.to_string(),
        );
        let inner_field =
            arrow_schema::Field::new("item", inner_storage, true).with_metadata(inner_metadata);
        let list_type = arrow_schema::DataType::List(Arc::new(inner_field));
        let arrow_field = arrow_schema::Field::new("tensor_list", list_type, true);

        let daft_field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(
            daft_field.dtype,
            DataType::List(Box::new(DataType::FixedShapeTensor(
                Box::new(DataType::Float32),
                vec![10, 10]
            )))
        );
    }
}
