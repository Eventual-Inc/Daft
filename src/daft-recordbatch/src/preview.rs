use std::fmt::{Display, Formatter};

use comfy_table::{CellAlignment, Table as ComfyTable};
use common_display::table_display::{StrValue, TableBuildOptions, TableColumnOptions, build_table};
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
#[serde(rename_all = "snake_case")]
pub enum PreviewFormat {
    /// Fancy box drawing.
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

/// The column alignment, auto is right for numbers otherwise left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviewAlign {
    Auto,
    Left,
    Center,
    Right,
}

/// The .show() options for column formatting.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
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
        Self {
            batch: preview,
            format,
            options,
        }
    }

    /// Create preview with default options.
    #[allow(unused)]
    pub fn default(preview: RecordBatch) -> Self {
        Self {
            batch: preview,
            format: PreviewFormat::default(),
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
            _ => Err(DaftError::ValueError(format!(
                "Unknown preview format: {s}"
            ))),
        }
    }
}

impl Default for PreviewFormat {
    fn default() -> Self {
        Self::Fancy
    }
}

/// The default options are meant to be backwards compatible with .show()
impl Default for PreviewOptions {
    fn default() -> Self {
        Self {
            columns: None,
            verbose: false,
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
            // repr_html means we are ignoring options..
            return write!(f, "{}", self.batch.repr_html());
        }
        self.create_table().fmt(f)
    }
}

/// Consider a preview builder, but this is fine for now.
impl Preview {
    /// Create a ComfyTable.
    fn create_table(&self) -> ComfyTable {
        let headers = self.headers();
        let column_options = self.column_options();
        let columns = self.columns();
        let preset = self.table_preset();
        let table_options = Self::table_build_options(preset);
        build_table(
            &headers,
            Some(&columns),
            Some(self.batch.len()),
            Some(&column_options),
            self.options.max_width,
            table_options,
        )
    }

    fn headers(&self) -> Vec<String> {
        self.batch
            .schema
            .into_iter()
            .enumerate()
            .map(|(idx, field)| {
                // Use schema for default header.
                let mut text = field.name.clone();
                let mut info = field.dtype.to_string();
                // Use column overrides if any.
                if let Some(overrides) = self.options.column(idx) {
                    text = overrides.header.clone().unwrap_or(text);
                    info = overrides.info.clone().unwrap_or(info);
                }
                // Create header cell with possible info.
                if self.options.verbose {
                    if self.format == PreviewFormat::Fancy {
                        format!("{text}\n---\n{info}")
                    } else {
                        format!("{text} ({info})")
                    }
                } else {
                    text
                }
            })
            .collect()
    }

    fn column_options(&self) -> Vec<TableColumnOptions> {
        self.batch
            .schema
            .into_iter()
            .enumerate()
            .map(|(idx, _)| {
                // Use column overrides if any, falling back to the global settings.
                let mut max_width = self.options.max_width;
                let mut align = self.options.align;
                if let Some(overrides) = self.options.column(idx) {
                    max_width = overrides.max_width.or(max_width);
                    align = overrides.align.or(align);
                }
                // If some alignment, translate to comfy_table
                let align = align.map(|align| match align {
                    PreviewAlign::Auto => CellAlignment::Left,
                    PreviewAlign::Left => CellAlignment::Left,
                    PreviewAlign::Center => CellAlignment::Center,
                    PreviewAlign::Right => CellAlignment::Right,
                });
                TableColumnOptions { max_width, align }
            })
            .collect()
    }

    fn columns(&self) -> Vec<&dyn StrValue> {
        self.batch
            .columns
            .iter()
            .map(|s| s as &dyn StrValue)
            .collect()
    }

    fn table_preset(&self) -> &'static str {
        match self.format {
            PreviewFormat::Fancy => presets::FANCY,
            PreviewFormat::Plain => presets::PLAIN,
            PreviewFormat::Simple => presets::SIMPLE,
            PreviewFormat::Grid => presets::GRID,
            PreviewFormat::Markdown => presets::MARKDOWN,
            _ => unreachable!("given format `{:?}` has no preset", self.format),
        }
    }

    fn table_build_options(preset: &'static str) -> TableBuildOptions {
        TableBuildOptions {
            preset,
            modifiers: &[],
            content_arrangement: None,
            use_terminal_width: false,
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
