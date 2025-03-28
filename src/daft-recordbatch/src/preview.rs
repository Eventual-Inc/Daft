use std::fmt::{Display, Formatter};

use comfy_table::{Cell, Table as ComfyTable};
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

/// The .show() table format for box drawing.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// The column alignment of some 
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreviewAlign {
    Left,
    Center,
    Right,
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
    /// Python f-string fmt like '.2f'
    fmt: Option<String>,
}

/// The .show() options for formatting.
#[derive(Debug, Serialize, Deserialize)]
pub struct PreviewOptions {
    /// Verbose will show info in the header.
    verbose: bool,
    /// The null string like 'None' or 'NULL'.
    null: String,
    /// Global max column width
    max_width: Option<usize>,
    /// Global column align
    align: Option<PreviewAlign>,
    /// Column overrides.
    columns: Option<Vec<PreviewColumn>>,
}

/// I have no intention of adding any more methods here to be honest.
impl Preview {
    /// Create preview with options.
    pub fn new(preview: RecordBatch, format: PreviewFormat, options: PreviewOptions) -> Self {
        Self { batch: preview, format, options }
    }

    /// Create preview with default options.
    #[allow(unused)]
    pub fn default(preview: RecordBatch) -> Self {
        Self {
            batch: preview,
            format: PreviewFormat::Fancy,
            options: PreviewOptions::default(),
        }
    }
}


impl PreviewOptions {
    /// Get the column preview settings or None.
    pub fn column(&self, idx: usize) -> Option<&PreviewColumn> {
        self.columns.as_ref().and_then(|cols| cols.get(idx))
    }
}

/// Could probably use some derive thing, but this is easy enough.
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

/// The default options are meant to be backwards compatible with .show()
impl Default for PreviewOptions {
    fn default() -> Self {
        Self {
            columns: None,
            verbose: true,
            null: "None".to_string(),
            max_width: Some(30),
            align: Some(PreviewAlign::Left),
        }
    }
}

impl Display for Preview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.format == PreviewFormat::Latex {
            todo!("latex preview not yet supported.")
        }
        if self.format == PreviewFormat::Html {
            todo!("html preview not yet supported.")
        }
        self.create_table().fmt(f)
    }
}

/// Consider a preview builder, but this is fine for now.
impl Preview {
    /// Create a ComfyTable.
    fn create_table(&self) -> ComfyTable {
        // 
        let mut table = ComfyTable::new();
        self.load_preset(&mut table);
        self.push_header(&mut table);
        self.push_body(&mut table);
        table
    }

    /// Daft has its own format presets.
    fn load_preset(&self, table: &mut ComfyTable) {
        let preset = match self.format {
            PreviewFormat::Fancy => presets::FANCY,
            PreviewFormat::Plain => presets::PLAIN,
            PreviewFormat::Simple => presets::SIMPLE,
            PreviewFormat::Grid => presets::GRID,
            PreviewFormat::Markdown => presets::MARKDOWN,
            _ => unreachable!("given format `{:?}` has no preset", self.format),
        };
        table.load_preset(preset);
    }

    /// Push the header row to the table.
    fn push_header(&self, table: &mut ComfyTable) {
        let row: Vec<Cell> = self.batch.schema.fields.iter().enumerate().map(|(i, field)| {
            //
            let mut text = field.1.name.to_string();
            let mut info = field.1.dtype.to_string();
            //
            if let Some(col) = self.options.column(i) {
                text = col.header.clone().unwrap_or(text);
                info = col.info.clone().unwrap_or(info);
            }
            // 
            self.create_header_cell(text, info)
        }).collect();
        table.set_header(row);
    }

    /// Push the body rows to the table.
    fn push_body(&self, _: &mut ComfyTable) {
        // no-op
    }

    /// Create a ComfyTable header cell.
    fn create_header_cell(&self, text: String, info: String) -> Cell {
        if !self.options.verbose {
            Cell::new(text)
        } else if self.format == PreviewFormat::Fancy {
            Cell::new(format!("{}\n---\n{}", text, info))
        } else {
            Cell::new(format!("{} ({})", text, info))
        }
    }
}

/// Preset strings for the text art tables.
mod presets {

    /// ```text
    /// ╭───────┬───────╮
    /// │ Hello ┆ there │
    /// ╞═══════╪═══════╡
    /// │ a     ┆ b     │
    /// ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    /// │ c     ┆ d     │
    /// ╰───────┴───────╯
    /// ```
    pub const FANCY: &str = "││──╞═╪╡┆╌┼├┤┬┴╭╮╰╯";

    /// ```text
    ///  Hello  there
    ///  a      b
    ///  c      d
    /// ```
    pub const PLAIN: &str = "                   ";

    /// ```text
    ///  Hello   there
    ///  ------- -----
    ///  a       b
    ///  c       d
    /// ```
    pub const SIMPLE: &str = "     -+            ";

    /// ```text
    /// ┌───────┬───────┐
    /// │ Hello │ there │
    /// ╞═══════╪═══════╡
    /// │ a     │ b     │
    /// ├───────┼───────┤
    /// │ c     │ d     │
    /// └───────┴───────┘
    /// ```
    pub const GRID: &str = "││──╞═╪╡│─┼├┤┬┴┌┐└┘";

    /// ```text
    /// | Hello | there |
    /// |-------|-------|
    /// | a     | b     |
    /// | c     | d     |
    /// ```
    pub const MARKDOWN: &str = "||  |-|||           ";
}
