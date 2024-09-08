pub use comfy_table;

const BOLD_TABLE_HEADERS_IN_DISPLAY: &str = "DAFT_BOLD_TABLE_HEADERS";

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

pub fn make_schema_vertical_table<S: ToString>(names: &[S], dtypes: &[S]) -> comfy_table::Table {
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
    assert_eq!(names.len(), dtypes.len());
    for (name, dtype) in names.iter().zip(dtypes.iter()) {
        table.add_row(vec![name.to_string(), dtype.to_string()]);
    }
    table
}

pub fn make_schema_horizontal_table<S: AsRef<str>>(fields: &[S]) -> comfy_table::Table {
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

    let max_cols = (((terminal_width + expected_col_width - 1) / expected_col_width) - 1).max(1);
    const DOTS: &str = "…";
    let num_columns = fields.len();

    let head_cols;
    let tail_cols;
    if num_columns > max_cols {
        head_cols = (max_cols + 1) / 2;
        tail_cols = max_cols / 2;
    } else {
        head_cols = num_columns;
        tail_cols = 0;
    }
    let mut header = fields
        .iter()
        .take(head_cols)
        .map(|field| create_table_cell(field.as_ref()))
        .collect::<Vec<_>>();
    if tail_cols > 0 {
        let unseen_cols = num_columns - (head_cols + tail_cols);
        header.push(
            create_table_cell(&format!(
                "{DOTS}\n\n({unseen_cols} hidden)",
                DOTS = DOTS,
                unseen_cols = unseen_cols
            ))
            .set_alignment(comfy_table::CellAlignment::Center),
        );
        header.extend(
            fields
                .iter()
                .skip(num_columns - tail_cols)
                .map(|field| create_table_cell(field.as_ref())),
        );
    }
    table.add_row(header);
    table
}
