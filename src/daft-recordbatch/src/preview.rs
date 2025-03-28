use std::fmt::{Display, Formatter};

// use comfy_table::Table as ComfyTable;
use common_error::DaftError;
use serde::{Deserialize, Serialize};

use crate::RecordBatch;

/// Preview struct so we can implement traits.
#[derive(Debug, Serialize, Deserialize)]
pub struct Preview {
    /// RecordBatch holds arcs and has a cheap clone, so use ownership here.
    batch: RecordBatch,
    /// Table drawing format.
    format: PreviewFormat,
    /// Used for indirection over comfy-table.
    options: PreviewOptions,
}

/// The .show() options for formatting.
#[derive(Debug, Serialize, Deserialize)]
pub struct PreviewOptions {
    /// Column rendering options.
    columns: Option<Vec<PreviewColumn>>,
    /// Verbose will show info in the header.
    verbose: bool,
    /// The null string like 'None' or 'NULL'.
    null: String,
}

/// The .show() table format for box drawing.
#[derive(Debug, Serialize, Deserialize)]
pub enum PreviewFormat {
    /// Fancy box drawing (default).
    Fancy,
    /// No box drawing.
    Plain,
    /// Pandoc simple_tables with only a header underline.
    Simple,
    /// Grid table with single-line box characters.
    Grid,
    /// Markdown formatted table, github flavor.
    Markdown,
    /// Latex formatted table.
    Latex,
    /// HTML formmated table.
    Html,
}

impl TryFrom<&str> for PreviewFormat {
    type Error = DaftError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "fancy" => Ok(Self::Fancy),
            "plain" => Ok(Self::Plain),
            "simple" => Ok(Self::Simple),
            "grid" => Ok(Self::Grid),
            "markdown" => Ok(Self::Markdown),
            "latex" => Ok(Self::Latex),
            "html" => Ok(Self::Html),
            _ => Err(DaftError::ValueError(format!("Unknown preview format: {}", s))),
        }
    }
}

/// The .show() options for column formatting.
#[derive(Debug, Serialize, Deserialize)]
pub struct PreviewColumn {
    /// Column header text.
    header: Option<String>,
    /// Column header info (optional).
    info: Option<String>,
    /// Max width
    max_width: Option<usize>,
    /// Column alignment
    align: Option<PreviewAlign>,
}

/// The column alignment of some 
#[derive(Debug, Serialize, Deserialize)]
pub enum PreviewAlign {
    Left,
    Center,
    Right,
}

/// I had no intention of adding any more methods here to be honest.
impl Preview {
    /// Create preview with options.
    pub fn new(preview: RecordBatch, format: PreviewFormat, options: PreviewOptions) -> Self {
        Self { batch: preview, format, options }
    }

    /// Create preview with default options.
    pub fn default(preview: RecordBatch) -> Self {
        Self {
            batch: preview,
            format: PreviewFormat::Fancy,
            options: PreviewOptions::default(),
        }
    }
}

/// The default options are meant to be backwards compatible with .show()
impl Default for PreviewOptions {
    fn default() -> Self {
        Self {
            columns: None,
            verbose: true,
            null: "None".to_string(),
        }
    }
}

impl Display for Preview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Hello from Preview display")
    }
}
