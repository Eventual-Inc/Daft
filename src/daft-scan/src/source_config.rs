use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::DatabaseSourceConfig;
use crate::FileFormatConfig;

/// Describes how a ScanTask produces data.
///
/// - `File` — read from files using a specific format parser
/// - `Database` — execute a SQL query against a connection
/// - `PythonFunction` — call a Python callable (DataSource/factory)
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum SourceConfig {
    File(FileFormatConfig),
    #[cfg(feature = "python")]
    Database(DatabaseSourceConfig),
    #[cfg(feature = "python")]
    PythonFunction {
        source_name: Option<String>,
        module_name: Option<String>,
        function_name: Option<String>,
    },
}

#[cfg(not(debug_assertions))]
impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.var_name())
    }
}

impl SourceConfig {
    #[must_use]
    pub fn var_name(&self) -> String {
        match self {
            Self::File(ffc) => ffc.var_name(),
            #[cfg(feature = "python")]
            Self::Database(_) => "Database".to_string(),
            #[cfg(feature = "python")]
            Self::PythonFunction {
                source_name,
                module_name,
                ..
            } => {
                if let Some(source_name) = source_name {
                    format!("{}(Python)", source_name)
                } else if let Some(module_name) = module_name {
                    format!("{}(Python)", module_name)
                } else {
                    "PythonFunction".to_string()
                }
            }
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::File(ffc) => ffc.multiline_display(),
            #[cfg(feature = "python")]
            Self::Database(source) => source.multiline_display(),
            #[cfg(feature = "python")]
            Self::PythonFunction {
                source_name,
                module_name,
                function_name,
            } => {
                let mut res = vec![];
                if let Some(source_name) = source_name {
                    res.push(format!("Source = {source_name}"));
                }
                if let Some(module_name) = module_name {
                    res.push(format!("Module = {module_name}"));
                }
                if let Some(function_name) = function_name {
                    res.push(format!("Function = {function_name}"));
                }
                res
            }
        }
    }
}
