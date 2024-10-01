pub use comfy_table;

const BOLD_TABLE_HEADERS_IN_DISPLAY: &str = "DAFT_BOLD_TABLE_HEADERS";

pub trait StrValue {
    fn str_value(&self, idx: usize) -> String;
}

pub trait HTMLValue {
    fn html_value(&self, idx: usize) -> String;
}

// this should be factored out to a common crate
fn create_table_cell(value: &str) -> comfy_table::Cell {
    let mut attributes = vec![];
    if std::env::var(BOLD_TABLE_HEADERS_IN_DISPLAY)
        .as_deref()
        .unwrap_or("1")
        == "1"
    {
        attributes.push(comfy_table::Attribute::Bold);
    }

    let mut cell = comfy_table::Cell::new(value);
    if !attributes.is_empty() {
        cell = cell.add_attributes(attributes);
    }
    cell
}

pub fn make_schema_vertical_table(
    fields: impl Iterator<Item = (String, String)>,
) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();

    let default_width_if_no_tty = 120usize;

    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    if table.width().is_none() && !table.is_tty() {
        table.set_width(default_width_if_no_tty as u16);
    }

    let header = vec![create_table_cell("Column Name"), create_table_cell("Type")];
    table.set_header(header);
    for (name, dtype) in fields {
        table.add_row(vec![name.clone(), dtype]);
    }
    table
}

pub fn make_comfy_table<S: AsRef<str>>(
    fields: &[S],
    columns: Option<&[&dyn StrValue]>,
    num_rows: Option<usize>,
    max_col_width: Option<usize>,
) -> comfy_table::Table {
    const DOTS: &str = "…";
    const TOTAL_ROWS: usize = 10;

    let mut table = comfy_table::Table::new();

    let default_width_if_no_tty = 120usize;

    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    if table.width().is_none() && !table.is_tty() {
        table.set_width(default_width_if_no_tty as u16);
    }
    let terminal_width = table
        .width()
        .expect("should have already been set with default") as usize;

    let expected_col_width = 18usize;

    let max_cols = (terminal_width.div_ceil(expected_col_width) - 1).max(1);
    let num_columns = fields.len();

    let (head_cols, tail_cols, total_cols) = if num_columns > max_cols {
        let head_cols = (max_cols + 1) / 2;
        let tail_cols = max_cols / 2;
        (head_cols, tail_cols, head_cols + tail_cols + 1)
    } else {
        (num_columns, 0, num_columns)
    };

    let mut header = fields
        .iter()
        .take(head_cols)
        .map(|field| create_table_cell(field.as_ref()))
        .collect::<Vec<_>>();
    if tail_cols > 0 {
        let unseen_cols = num_columns - (head_cols + tail_cols);
        header.push(
            create_table_cell(&format!("{DOTS}\n\n({unseen_cols} hidden)"))
                .set_alignment(comfy_table::CellAlignment::Center),
        );
        header.extend(
            fields
                .iter()
                .skip(num_columns - tail_cols)
                .map(|field| create_table_cell(field.as_ref())),
        );
    }

    if let Some(columns) = columns
        && !columns.is_empty()
    {
        table.set_header(header);
        let len = num_rows.expect("if columns are set, so should `num_rows`");
        let (head_rows, tail_rows) = if len > TOTAL_ROWS {
            (TOTAL_ROWS / 2, TOTAL_ROWS / 2)
        } else {
            (len, 0)
        };

        for i in 0..head_rows {
            let all_cols = columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i);
                    if let Some(max_col_width) = max_col_width {
                        if str_val.len() > max_col_width - DOTS.len() {
                            str_val = format!(
                                "{}{DOTS}",
                                &str_val
                                    .char_indices()
                                    .take(max_col_width - DOTS.len())
                                    .map(|(_, c)| c)
                                    .collect::<String>()
                            );
                        }
                    }
                    str_val
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
        if tail_rows != 0 {
            table.add_row((0..total_cols).map(|_| DOTS).collect::<Vec<_>>());
        }

        for i in (len - tail_rows)..(len) {
            let all_cols = columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i);
                    if let Some(max_col_width) = max_col_width {
                        if str_val.len() > max_col_width - DOTS.len() {
                            str_val = format!(
                                "{}{DOTS}",
                                &str_val
                                    .char_indices()
                                    .take(max_col_width - DOTS.len())
                                    .map(|(_, c)| c)
                                    .collect::<String>()
                            );
                        }
                    }
                    str_val
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
