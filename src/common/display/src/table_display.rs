pub use comfy_table;

const DEFAULT_WIDTH_IF_NO_TTY: u16 = 120;
const EXPECTED_COL_WIDTH: usize = 18;
const DOTS: &str = "…";

pub trait StrValue {
    fn str_value(&self, idx: usize) -> String;
}

pub trait HTMLValue {
    fn html_value(&self, idx: usize) -> String;
}

fn is_pytest() -> bool {
    std::env::var_os("PYTEST_CURRENT_TEST").is_some()
}

fn maybe_apply_width(table: &mut comfy_table::Table) {
    if is_pytest() {
        table.force_no_tty();
    }
    if table.width().is_none() && !table.is_tty() {
        table.set_width(DEFAULT_WIDTH_IF_NO_TTY);
    }
}

fn create_table_cell(value: &str) -> comfy_table::Cell {
    let mut cell = comfy_table::Cell::new(value);
    if !is_pytest() {
        cell = cell.add_attribute(comfy_table::Attribute::Bold);
    }
    cell
}

pub fn make_schema_vertical_table(
    fields: impl Iterator<Item = (String, String, String)>,
) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    maybe_apply_width(&mut table);

    let fields: Vec<_> = fields.collect();
    let has_metadata = fields.iter().any(|(_, _, meta)| !meta.is_empty());
    let mut header = vec![create_table_cell("column_name"), create_table_cell("type")];
    if has_metadata {
        header.push(create_table_cell("metadata"));
    }
    table.set_header(header);

    for (name, dtype, metadata) in fields {
        if has_metadata {
            table.add_row(vec![name, dtype, metadata]);
        } else {
            table.add_row(vec![name, dtype]);
        }
    }
    table
}

#[derive(Clone, Copy)]
pub struct TableColumnOptions {
    pub max_width: Option<usize>,
    pub align: Option<comfy_table::CellAlignment>,
}

#[derive(Clone)]
pub struct TableBuildOptions {
    pub preset: &'static str,
    pub modifiers: &'static [&'static str],
    pub content_arrangement: Option<comfy_table::ContentArrangement>,
    pub use_terminal_width: bool,
}

impl Default for TableBuildOptions {
    fn default() -> Self {
        Self {
            preset: comfy_table::presets::UTF8_FULL,
            modifiers: &[comfy_table::modifiers::UTF8_ROUND_CORNERS],
            content_arrangement: Some(comfy_table::ContentArrangement::Dynamic),
            use_terminal_width: true,
        }
    }
}

fn build_header_cells<S: AsRef<str>>(
    fields: &[S],
    head_cols: usize,
    tail_cols: usize,
) -> Vec<comfy_table::Cell> {
    let mut header = fields
        .iter()
        .take(head_cols)
        .map(|field| create_table_cell(field.as_ref()))
        .collect::<Vec<_>>();

    if tail_cols > 0 {
        let unseen_cols = fields.len() - (head_cols + tail_cols);
        let ellipsis = format!("{DOTS}\n\n({unseen_cols} hidden)");
        header.push(create_table_cell(&ellipsis).set_alignment(comfy_table::CellAlignment::Center));
        header.extend(
            fields
                .iter()
                .skip(fields.len() - tail_cols)
                .map(|field| create_table_cell(field.as_ref())),
        );
    }

    header
}

pub fn build_table<S: AsRef<str>>(
    fields: &[S],
    columns: Option<&[&dyn StrValue]>,
    num_rows: Option<usize>,
    column_options: Option<&[TableColumnOptions]>,
    global_max_width: Option<usize>,
    options: TableBuildOptions,
) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();

    table.load_preset(options.preset);
    for modifier in options.modifiers {
        table.apply_modifier(modifier);
    }
    if let Some(arrangement) = options.content_arrangement {
        table.set_content_arrangement(arrangement);
    }
    maybe_apply_width(&mut table);

    let num_columns = fields.len();
    let (head_cols, tail_cols) = if options.use_terminal_width {
        let terminal_width = table
            .width()
            .expect("should have already been set with default")
            as usize;
        let max_cols = (terminal_width.div_ceil(EXPECTED_COL_WIDTH) - 1).max(1);
        if num_columns > max_cols {
            (max_cols.div_ceil(2), max_cols / 2)
        } else {
            (num_columns, 0)
        }
    } else {
        (num_columns, 0)
    };

    let header = build_header_cells(fields, head_cols, tail_cols);
    if let Some(columns) = columns
        && !columns.is_empty()
    {
        table.set_header(header);
        let len = num_rows.expect("if columns are set, so should `num_rows`");

        for i in 0..len {
            let all_cols = columns
                .iter()
                .enumerate()
                .map(|(idx, s)| {
                    let mut str_val = s.str_value(i);
                    let max_width = column_options
                        .and_then(|opts| opts.get(idx))
                        .and_then(|opts| opts.max_width)
                        .or(global_max_width);

                    if let Some(max_width) = max_width
                        && str_val.char_indices().count() > max_width - DOTS.len()
                    {
                        str_val = format!(
                            "{}{DOTS}",
                            &str_val
                                .char_indices()
                                .take(max_width - DOTS.len())
                                .map(|(_, c)| c)
                                .collect::<String>()
                        );
                    }
                    let mut cell = comfy_table::Cell::new(str_val);
                    if let Some(align) = column_options
                        .and_then(|opts| opts.get(idx))
                        .and_then(|opts| opts.align)
                    {
                        cell = cell.set_alignment(align);
                    }
                    cell
                })
                .collect::<Vec<_>>();

            if tail_cols > 0 {
                let mut final_row = all_cols.iter().take(head_cols).cloned().collect::<Vec<_>>();
                final_row.push(DOTS.into());
                final_row.extend(all_cols.iter().skip(num_columns - tail_cols).cloned());
                table.add_row(final_row);
            } else {
                table.add_row(all_cols);
            }
        }
    } else {
        table.add_row(header);
    }

    table
}

pub fn make_comfy_table<S: AsRef<str>>(
    fields: &[S],
    columns: Option<&[&dyn StrValue]>,
    num_rows: Option<usize>,
    max_col_width: Option<usize>,
) -> comfy_table::Table {
    build_table(
        fields,
        columns,
        num_rows,
        None,
        max_col_width,
        TableBuildOptions::default(),
    )
}
