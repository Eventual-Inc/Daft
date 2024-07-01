use crate::{
    datatypes::{Field, TimeUnit},
    Series,
};
use common_daft_config::BOLD_TABLE_HEADERS_IN_DISPLAY;
use itertools::Itertools;

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

pub fn display_date32(val: i32) -> String {
    let epoch_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = if val.is_positive() {
        epoch_date + chrono::naive::Days::new(val as u64)
    } else {
        epoch_date - chrono::naive::Days::new(val.unsigned_abs() as u64)
    };
    format!("{date}")
}

pub fn display_time64(val: i64, unit: &TimeUnit) -> String {
    let time = match unit {
        TimeUnit::Nanoseconds => Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (val / 1_000_000_000) as u32,
            (val % 1_000_000_000) as u32,
        )
        .unwrap()),
        TimeUnit::Microseconds => Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (val / 1_000_000) as u32,
            ((val % 1_000_000) * 1_000) as u32,
        )
        .unwrap()),
        TimeUnit::Milliseconds => {
            let seconds = u32::try_from(val / 1_000);
            let nanoseconds = u32::try_from((val % 1_000) * 1_000_000);
            match (seconds, nanoseconds) {
                (Ok(secs), Ok(nano)) => {
                    Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap())
                }
                (Err(e), _) => Err(e),
                (_, Err(e)) => Err(e),
            }
        }
        TimeUnit::Seconds => {
            let seconds = u32::try_from(val);
            match seconds {
                Ok(secs) => {
                    Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap())
                }
                Err(e) => Err(e),
            }
        }
    };

    match time {
        Ok(time) => format!("{time}"),
        Err(e) => format!("Display Error: {e}"),
    }
}

pub fn display_timestamp(val: i64, unit: &TimeUnit, timezone: &Option<String>) -> String {
    use crate::array::ops::cast::{
        timestamp_to_str_naive, timestamp_to_str_offset, timestamp_to_str_tz,
    };

    timezone.as_ref().map_or_else(
        || timestamp_to_str_naive(val, unit),
        |timezone| {
            // In arrow, timezone string can be either:
            // 1. a fixed offset "-07:00", parsed using parse_offset, or
            // 2. a timezone name e.g. "America/Los_Angeles", parsed using parse_offset_tz.
            if let Ok(offset) = arrow2::temporal_conversions::parse_offset(timezone) {
                timestamp_to_str_offset(val, unit, &offset)
            } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(timezone) {
                timestamp_to_str_tz(val, unit, &tz)
            } else {
                panic!("Unable to parse timezone string {}", timezone)
            }
        },
    )
}

pub fn display_decimal128(val: i128, _precision: u8, scale: i8) -> String {
    if scale < 0 {
        unimplemented!();
    } else {
        let modulus = i128::pow(10, scale as u32);
        let integral = val / modulus;
        if scale == 0 {
            format!("{}", integral)
        } else {
            let sign = if val < 0 { "-" } else { "" };
            let integral = integral.abs();
            let decimals = (val % modulus).abs();
            let scale = scale as usize;
            format!("{}{}.{:0scale$}", sign, integral, decimals)
        }
    }
}

pub fn display_series_literal(series: &Series) -> String {
    if !series.is_empty() {
        format!(
            "[{}]",
            (0..series.len())
                .map(|i| series.str_value(i).unwrap())
                .join(", ")
        )
    } else {
        "[]".to_string()
    }
}

pub fn make_schema_vertical_table<F: AsRef<Field>>(fields: &[F]) -> comfy_table::Table {
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

    for f in fields.iter() {
        table.add_row(vec![
            f.as_ref().name.to_string(),
            format!("{}", f.as_ref().dtype),
        ]);
    }
    table
}

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
            create_table_cell(&format!(
                "{}\n---\n{}",
                field.as_ref().name,
                field.as_ref().dtype
            ))
        })
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
        header.extend(fields.iter().skip(num_columns - tail_cols).map(|field| {
            create_table_cell(&format!(
                "{}\n---\n{}",
                field.as_ref().name,
                field.as_ref().dtype
            ))
        }));
    }

    if let Some(columns) = columns
        && !columns.is_empty()
    {
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
            let all_cols = columns
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
