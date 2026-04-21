use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Default, Clone, Display, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Dimension {
    #[default]
    XY,
    XYZ,
    XYM,
    XYZM,
}

impl Dimension {
    pub fn num_coords(&self) -> usize {
        match self {
            Self::XY => 2,
            Self::XYZ => 3,
            Self::XYM => 3,
            Self::XYZM => 4,
        }
    }

    pub fn from_user_dimension(dimension: &str) -> DaftResult<Self> {
        match dimension {
            "xy" => Ok(Self::XY),
            "xyz" => Ok(Self::XYZ),
            "xym" => Ok(Self::XYM),
            "xyzm" => Ok(Self::XYZM),
            _ => Err(DaftError::TypeError(format!(
                "unsupported dimension: {dimension}, daft only supports xy, xyz, xym, xyzm"
            ))),
        }
    }
}

#[derive(Debug, Default, Display, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CoordType {
    #[default]
    Separated,
    Interleaved,
}

impl CoordType {
    pub fn from_user_coord_type(coord_type: &str) -> DaftResult<Self> {
        match coord_type {
            "interleaved" => Ok(Self::Interleaved),
            "separated" => Ok(Self::Separated),
            _ => Err(DaftError::TypeError(format!(
                "unsupported coord type: {coord_type}, daft only supports interleaved and separated"
            ))),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Crs {
    pub crs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs_type: Option<CrsType>,
}

impl Crs {
    pub fn new(crs: Option<Value>, crs_type: Option<CrsType>) -> Self {
        Self { crs, crs_type }
    }
}

impl std::fmt::Display for Crs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.crs {
            None => write!(f, "Crs: None")?,
            Some(Value::String(s)) => write!(f, "Crs: {s}")?,
            Some(v) => write!(f, "Crs: {v}")?,
        }
        match &self.crs_type {
            None => write!(f, ", CrsType: None"),
            Some(ct) => write!(f, ", CrsType: {ct}"),
        }
    }
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CrsType {
    #[serde(rename = "projjson")]
    Projjson,
    #[serde(rename = "wkt2:2019")]
    Wkt2_2019,
    #[serde(rename = "authority_code")]
    AuthorityCode,
    #[serde(rename = "srid")]
    Srid,
}

impl CrsType {
    pub fn from_user_crs_type(crs_type: &str) -> DaftResult<Self> {
        match crs_type {
            "projjson" => Ok(Self::Projjson),
            "wkt2:2019" => Ok(Self::Wkt2_2019),
            "authority_code" => Ok(Self::AuthorityCode),
            "srid" => Ok(Self::Srid),
            _ => Err(DaftError::TypeError(format!(
                "unsupported crs type: {crs_type}, daft only supports projjson, wkt2:2019, authority_code and srid"
            ))),
        }
    }
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Edges {
    #[serde(rename = "andoyer")]
    Andoyer,
    #[serde(rename = "karney")]
    Karney,
    #[serde(rename = "spherical")]
    Spherical,
    #[serde(rename = "thomas")]
    Thomas,
    #[serde(rename = "vincenty")]
    Vincenty,
}

impl Edges {
    pub fn from_user_edges(edges: &str) -> DaftResult<Self> {
        match edges {
            "andoyer" => Ok(Self::Andoyer),
            "karney" => Ok(Self::Karney),
            "spherical" => Ok(Self::Spherical),
            "thomas" => Ok(Self::Thomas),
            "vincenty" => Ok(Self::Vincenty),
            _ => Err(DaftError::TypeError(format!(
                "unsupported edges: {edges}, daft only supports andoyer, karney, spherical, thomas and vincenty"
            ))),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Metadata {
    #[serde(flatten)]
    pub crs: Crs,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges: Option<Edges>,
}

impl Metadata {
    pub fn new(crs: Crs, edges: Option<Edges>) -> Self {
        Self { crs, edges }
    }
}

impl std::fmt::Display for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.edges {
            None => write!(f, "{}, Edges: None", self.crs),
            Some(e) => write!(f, "{}, Edges: {e}", self.crs),
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(
    feature = "python",
    pyclass(name = "PyGeospatialMode", module = "daft.daft", eq, from_py_object)
)]
pub struct GeospatialMode {
    pub metadata: Metadata,
    pub dimension: Dimension,
    pub coord_type: CoordType,
}

impl std::fmt::Display for GeospatialMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}, {}, {}",
            self.dimension, self.coord_type, self.metadata
        )
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl GeospatialMode {
    pub fn __repr__(&self) -> String {
        format!("{self}")
    }

    pub fn __hash__(&self) -> u64 {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    #[staticmethod]
    #[pyo3(signature = (dimension, coord_type, crs=None, crs_type=None, edges=None))]
    pub fn from_user_defined_mode(
        dimension: &str,
        coord_type: &str,
        crs: Option<&str>,
        crs_type: Option<&str>,
        edges: Option<&str>,
    ) -> PyResult<Self> {
        Self::from_user_spec(dimension, coord_type, crs, crs_type, edges)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

impl GeospatialMode {
    pub fn new(metadata: Metadata, dimension: Dimension, coord_type: CoordType) -> Self {
        Self {
            metadata,
            dimension,
            coord_type,
        }
    }

    pub fn from_user_spec(
        dimension: &str,
        coord_type: &str,
        crs: Option<&str>,
        crs_type: Option<&str>,
        edges: Option<&str>,
    ) -> DaftResult<Self> {
        let dimension = Dimension::from_user_dimension(dimension)?;
        let coord_type = CoordType::from_user_coord_type(coord_type)?;
        let crs_value = crs.map(|s| {
            serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string()))
        });
        let crs_type = crs_type.map(CrsType::from_user_crs_type).transpose()?;
        let edges = edges.map(Edges::from_user_edges).transpose()?;
        let metadata = Metadata::new(Crs::new(crs_value, crs_type), edges);
        Ok(Self::new(metadata, dimension, coord_type))
    }
}

impl_bincode_py_state_serialization!(GeospatialMode);
