//! Numeric range expansion for glob patterns.
//!
//! This module provides functionality to expand numeric range patterns like `{0..10}`
//! into alternation patterns like `{0,1,2,3,4,5,6,7,8,9,10}` that are compatible with
//! the `globset` crate.
//!
//! # Examples
//!
//! ```ignore
//! use daft_io::expand_numeric_ranges;
//!
//! // Simple range
//! let result = expand_numeric_ranges("s3://bucket/{0..3}.parquet")?;
//! assert_eq!(result, "s3://bucket/{0,1,2,3}.parquet");
//!
//! // With leading zeros
//! let result = expand_numeric_ranges("s3://bucket/{00..03}.parquet")?;
//! assert_eq!(result, "s3://bucket/{00,01,02,03}.parquet");
//!
//! // Reverse range
//! let result = expand_numeric_ranges("s3://bucket/{3..0}.parquet")?;
//! assert_eq!(result, "s3://bucket/{3,2,1,0}.parquet");
//! ```

use std::sync::LazyLock;

use regex::Regex;

/// Maximum number of elements allowed in a single range expansion.
/// This prevents memory issues from patterns like `{0..1000000}`.
const MAX_RANGE_SIZE: usize = 10_000;

/// Regex pattern to match numeric range syntax: {start..end}
/// Supports optional leading zeros and negative numbers.
/// Examples: {0..10}, {00..99}, {-5..5}
static NUMERIC_RANGE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    // Match {start..end} where start and end are integers (possibly with leading zeros or negative)
    // We need to be careful not to match alternation syntax like {a,b,c}
    Regex::new(r"\{(-?\d+)\.\.(-?\d+)\}").expect("Invalid regex pattern for numeric range")
});

/// Expands all numeric range patterns in a glob string.
///
/// Converts patterns like `{0..10}` to `{0,1,2,3,4,5,6,7,8,9,10}`.
///
/// # Arguments
///
/// * `glob` - The glob pattern string that may contain numeric ranges
///
/// # Returns
///
/// * `Ok(String)` - The expanded glob pattern
/// * `Err(Error)` - If a range is too large (exceeds MAX_RANGE_SIZE)
///
/// # Examples
///
/// Basic usage:
/// ```ignore
/// let expanded = expand_numeric_ranges("data/{0..2}.csv")?;
/// assert_eq!(expanded, "data/{0,1,2}.csv");
/// ```
pub fn expand_numeric_ranges(glob: &str) -> super::Result<String> {
    // If no numeric range pattern found, return as-is
    if !NUMERIC_RANGE_PATTERN.is_match(glob) {
        return Ok(glob.to_string());
    }

    // We need to expand ranges one at a time because each expansion might
    // change the string length and positions
    let mut result = glob.to_string();

    // Keep expanding until no more ranges are found
    // This handles multiple ranges in a single glob pattern
    while let Some(captures) = NUMERIC_RANGE_PATTERN.captures(&result) {
        let full_match = captures.get(0).unwrap();
        let start_str = captures.get(1).unwrap().as_str();
        let end_str = captures.get(2).unwrap().as_str();

        let expansion = expand_single_range(start_str, end_str)?;

        // Replace the matched range with the expansion
        result = format!(
            "{}{{{}}}{}",
            &result[..full_match.start()],
            expansion,
            &result[full_match.end()..]
        );
    }

    Ok(result)
}

/// Expands a single numeric range into a comma-separated list.
///
/// # Arguments
///
/// * `start_str` - The start value as a string (preserves leading zeros)
/// * `end_str` - The end value as a string (preserves leading zeros)
///
/// # Returns
///
/// A comma-separated string of all values in the range.
fn expand_single_range(start_str: &str, end_str: &str) -> super::Result<String> {
    let start: i64 = start_str
        .parse()
        .map_err(|_| super::Error::InvalidArgument {
            msg: format!("Invalid range start value: {}", start_str),
        })?;

    let end: i64 = end_str.parse().map_err(|_| super::Error::InvalidArgument {
        msg: format!("Invalid range end value: {}", end_str),
    })?;

    // Calculate the range size using i128 to avoid overflow
    // when start and end are at opposite extremes of i64 range
    let range_size = ((end as i128) - (start as i128)).unsigned_abs() as usize + 1;
    if range_size > MAX_RANGE_SIZE {
        return Err(super::Error::InvalidArgument {
            msg: format!(
                "Numeric range {{{}..{}}} would expand to {} elements, which exceeds the maximum of {}. \
                 Consider using a smaller range or Python list comprehension instead.",
                start_str, end_str, range_size, MAX_RANGE_SIZE
            ),
        });
    }

    // Determine the width for zero-padding based on the longer of start or end
    // Only apply padding if either value has leading zeros
    let start_has_leading_zero = start_str.starts_with('0') && start_str.len() > 1 && start >= 0;
    let end_has_leading_zero = end_str.starts_with('0') && end_str.len() > 1 && end >= 0;
    let use_padding = start_has_leading_zero || end_has_leading_zero;

    let width = if use_padding {
        // Use the maximum width between start and end strings
        // For negative numbers, we don't count the minus sign for padding purposes
        let start_width = if start < 0 {
            start_str.len() - 1
        } else {
            start_str.len()
        };
        let end_width = if end < 0 {
            end_str.len() - 1
        } else {
            end_str.len()
        };
        start_width.max(end_width)
    } else {
        0
    };

    // Generate the range values
    let values: Vec<String> = if start <= end {
        // Ascending range
        (start..=end)
            .map(|n| format_number(n, width, use_padding))
            .collect()
    } else {
        // Descending range
        (end..=start)
            .rev()
            .map(|n| format_number(n, width, use_padding))
            .collect()
    };

    Ok(values.join(","))
}

/// Formats a number with optional zero-padding.
fn format_number(n: i64, width: usize, use_padding: bool) -> String {
    if use_padding && n >= 0 {
        format!("{:0>width$}", n, width = width)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_expansion_needed() {
        let result = expand_numeric_ranges("s3://bucket/*.parquet").unwrap();
        assert_eq!(result, "s3://bucket/*.parquet");
    }

    #[test]
    fn test_simple_range() {
        let result = expand_numeric_ranges("s3://bucket/{0..3}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{0,1,2,3}.parquet");
    }

    #[test]
    fn test_leading_zeros() {
        let result = expand_numeric_ranges("s3://bucket/{00..03}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{00,01,02,03}.parquet");
    }

    #[test]
    fn test_leading_zeros_larger() {
        let result = expand_numeric_ranges("s3://bucket/{000..005}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{000,001,002,003,004,005}.parquet");
    }

    #[test]
    fn test_reverse_range() {
        let result = expand_numeric_ranges("s3://bucket/{3..0}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{3,2,1,0}.parquet");
    }

    #[test]
    fn test_negative_range() {
        let result = expand_numeric_ranges("s3://bucket/{-2..1}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{-2,-1,0,1}.parquet");
    }

    #[test]
    fn test_single_value_range() {
        let result = expand_numeric_ranges("s3://bucket/{5..5}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{5}.parquet");
    }

    #[test]
    fn test_multiple_ranges() {
        let result = expand_numeric_ranges("s3://bucket/{0..1}/{0..2}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{0,1}/{0,1,2}.parquet");
    }

    #[test]
    fn test_mixed_with_alternation() {
        // Alternation syntax {a,b} should remain untouched
        let result = expand_numeric_ranges("s3://bucket/{0..1}_{a,b}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{0,1}_{a,b}.parquet");
    }

    #[test]
    fn test_range_in_middle_of_path() {
        let result = expand_numeric_ranges("s3://bucket/data_{0..2}_suffix.parquet").unwrap();
        assert_eq!(result, "s3://bucket/data_{0,1,2}_suffix.parquet");
    }

    #[test]
    fn test_local_path() {
        let result = expand_numeric_ranges("/local/path/{0..2}.csv").unwrap();
        assert_eq!(result, "/local/path/{0,1,2}.csv");
    }

    #[test]
    fn test_http_url() {
        let result = expand_numeric_ranges("https://example.com/{0..2}.json").unwrap();
        assert_eq!(result, "https://example.com/{0,1,2}.json");
    }

    #[test]
    fn test_range_too_large() {
        let result = expand_numeric_ranges("s3://bucket/{0..100000}.parquet");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("exceeds the maximum"));
    }

    #[test]
    fn test_no_change_for_alternation() {
        // Pure alternation syntax should pass through unchanged
        let result = expand_numeric_ranges("s3://bucket/{foo,bar,baz}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{foo,bar,baz}.parquet");
    }

    #[test]
    fn test_asymmetric_padding() {
        // When one side has padding and other doesn't, use the larger width
        let result = expand_numeric_ranges("s3://bucket/{08..12}.parquet").unwrap();
        assert_eq!(result, "s3://bucket/{08,09,10,11,12}.parquet");
    }

    #[test]
    fn test_escaped_range_not_expanded() {
        // Escaped braces should NOT be expanded
        // In Rust string, \{ is represented as \\{
        let result = expand_numeric_ranges(r"s3://bucket/\{0..3\}.parquet").unwrap();
        assert_eq!(result, r"s3://bucket/\{0..3\}.parquet");
    }
}
