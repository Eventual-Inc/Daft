mod file_format;
pub use file_format::FileFormat;

mod file_format_config;
#[cfg(feature = "python")]
pub use file_format_config::DatabaseSourceConfig;
pub use file_format_config::{
    CsvSourceConfig, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig,
};

#[cfg(feature = "python")]
pub mod python;

impl From<&FileFormatConfig> for FileFormat {
    fn from(file_format_config: &FileFormatConfig) -> Self {
        match file_format_config {
            FileFormatConfig::Parquet(_) => Self::Parquet,
            FileFormatConfig::Csv(_) => Self::Csv,
            FileFormatConfig::Json(_) => Self::Json,
            #[cfg(feature = "python")]
            FileFormatConfig::Database(_) => Self::Database,
            #[cfg(feature = "python")]
            FileFormatConfig::PythonFunction => Self::Python,
        }
    }
}
