use std::fmt::{Display, Formatter};

use comfy_table::{Cell, CellAlignment, Table as ComfyTable};
use common_display::table_display::StrValue;
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
            _ => Err(DaftError::ValueError(format!(
                "Unknown preview format: {}",
                s
            ))),
        }
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
        let row: Vec<Cell> = self
            .batch
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
                if !self.options.verbose {
                    Cell::new(text)
                } else if self.format == PreviewFormat::Fancy {
                    Cell::new(format!("{}\n---\n{}", text, info))
                } else {
                    Cell::new(format!("{} ({})", text, info))
                }
            })
            .collect();
        table.set_header(row);
    }

    /// Push the body rows to the table.
    fn push_body(&self, table: &mut ComfyTable) {
        let total_rows = self.batch.len();

        // Add target rows
        for o in 0..total_rows {
            let row: Vec<Cell> = self
                .batch
                .columns
                .iter()
                .enumerate()
                .map(|(idx, series)| {
                    // Unfortunately actually plumbing the formatting would make this too much work right now.
                    let mut text = series.str_value(o);
                    let mut alignment = CellAlignment::Left;

                    // Use column overrides if any, falling back to the global settings.
                    let mut max_width = self.options.max_width;
                    let mut align: Option<PreviewAlign> = self.options.align;
                    if let Some(overrides) = self.options.column(idx) {
                        max_width = overrides.max_width.or(max_width);
                        align = overrides.align.or(align);
                    }

                    // Truncate cell content if over max length.
                    if let Some(max_width) = max_width {
                        text = truncate(text, max_width, magic::DOTS);
                    }

                    // If some alignment, translate to comfy_table
                    if let Some(align) = &align {
                        alignment = match align {
                            PreviewAlign::Auto => CellAlignment::Left,
                            PreviewAlign::Left => CellAlignment::Left,
                            PreviewAlign::Center => CellAlignment::Center,
                            PreviewAlign::Right => CellAlignment::Right,
                        };
                    }

                    // Use global overrides
                    Cell::new(text).set_alignment(alignment)
                })
                .collect();
            table.add_row(row);
        }
    }
}

/// Truncate a string, appending the
fn truncate(text: String, max_width: usize, suffix: &str) -> String {
    if text.len() < max_width {
        return text;
    }
    format!(
        "{}{suffix}",
        &text
            .char_indices()
            .take(max_width - suffix.len())
            .map(|(_, c)| c)
            .collect::<String>()
    )
}

/// Formatting magic strings.
mod magic {
    /// Fancy dots aka ellipse for truncated columns.
    pub const DOTS: &str = "…";
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
