pub fn bytes_to_human_readable(byte_count: usize) -> String {
    if byte_count == 0 {
        return "0 B".to_string();
    }

    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    let base = byte_count.ilog2() / 10;

    let index = std::cmp::min(base, (UNITS.len() - 1) as u32);

    let scaled_value = (byte_count as f64) / (1024f64.powf(index as f64));
    let unit = UNITS.get(index as usize).unwrap();
    if index == 0 {
        format!("{byte_count} {unit}")
    } else {
        format!("{scaled_value:.2} {unit}")
    }
}
