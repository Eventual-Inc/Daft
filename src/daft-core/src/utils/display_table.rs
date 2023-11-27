use crate::{datatypes::Field, Series};

pub fn make_comfy_table<F: AsRef<Field>>(
    fields: &[F],
    columns: Option<&[&Series]>,
    max_col_width: Option<usize>,
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
    let terminal_width = table
        .width()
        .expect("should have already been set with default") as usize;

    let expected_col_width = 18usize;

    let max_cols = (((terminal_width + expected_col_width - 1) / expected_col_width) - 1).max(1);
    const DOTS: &str = "…";
    let num_columns = fields.len();

    let head_cols;
    let tail_cols;
    let total_cols;
    if num_columns > max_cols {
        head_cols = (max_cols + 1) / 2;
        tail_cols = max_cols / 2;
        total_cols = head_cols + tail_cols + 1;
    } else {
        head_cols = num_columns;
        tail_cols = 0;
        total_cols = head_cols;
    }
    let mut header = fields
        .iter()
        .take(head_cols)
        .map(|field| {
            comfy_table::Cell::new(
                format!("{}\n---\n{}", field.as_ref().name, field.as_ref().dtype).as_str(),
            )
            .add_attribute(comfy_table::Attribute::Bold)
        })
        .collect::<Vec<_>>();
    if tail_cols > 0 {
        let unseen_cols = num_columns - (head_cols + tail_cols);
        header.push(
            comfy_table::Cell::new(format!("{DOTS}\n\n({unseen_cols} hidden)"))
                .add_attribute(comfy_table::Attribute::Bold)
                .set_alignment(comfy_table::CellAlignment::Center),
        );
        header.extend(fields.iter().skip(num_columns - tail_cols).map(|field| {
            comfy_table::Cell::new(
                format!("{}\n---\n{}", field.as_ref().name, field.as_ref().dtype).as_str(),
            )
            .add_attribute(comfy_table::Attribute::Bold)
        }))
    }

    if let Some(columns) = columns && !columns.is_empty() {
        table.set_header(header);
        let len = columns.first().unwrap().len();
        const TOTAL_ROWS: usize = 10;
        let head_rows;
        let tail_rows;

        if len > TOTAL_ROWS {
            head_rows = TOTAL_ROWS / 2;
            tail_rows = TOTAL_ROWS / 2;
        } else {
            head_rows = len;
            tail_rows = 0;
        }



        for i in 0..head_rows {
            let all_cols  = columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i).unwrap();
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
                }).collect::<Vec<_>>();

            if tail_cols > 0 {
                let mut final_row = all_cols.iter().take(head_cols).cloned().collect::<Vec<_>>();
                final_row.push(DOTS.into());
                final_row.extend(
                    all_cols
                    .iter()
                    .skip(num_columns - tail_cols)
                    .cloned()
                );
                table.add_row(final_row);

            } else {
                table.add_row(all_cols);
            }


        }
        if tail_rows != 0 {
            table.add_row((0..total_cols).map(|_| DOTS).collect::<Vec<_>>());
        }

        for i in (len - tail_rows)..(len) {
            let all_cols  = columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i).unwrap();
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
                }).collect::<Vec<_>>();

            if tail_cols > 0 {
                let mut final_row = all_cols.iter().take(head_cols).cloned().collect::<Vec<_>>();
                final_row.push(DOTS.into());
                final_row.extend(
                    all_cols
                    .iter()
                    .skip(num_columns - tail_cols)
                    .cloned()
                );
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
