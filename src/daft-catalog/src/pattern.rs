use common_pattern::like_pattern_to_regex;
use regex::Regex;

/// Parsed components of a qualified table pattern.
///
/// Examples:
/// - "table%" → QualifiedPattern { namespace: None, table: "table%" }
/// - "ns1.table%" → QualifiedPattern { namespace: Some("ns1"), table: "table%" }
/// - "ns1.%" → QualifiedPattern { namespace: Some("ns1"), table: "%" }
#[derive(Debug, PartialEq)]
pub(crate) struct QualifiedPattern<'a> {
    /// Optional namespace filter (e.g., "ns1")
    pub namespace: Option<&'a str>,
    /// Table name pattern (e.g., "table%", "%")
    pub table: &'a str,
}

/// Parses a pattern string into namespace and table components.
///
/// Supports qualified patterns using dot notation:
/// - "table%" → matches tables in any namespace
/// - "ns1.table%" → matches tables in namespace "ns1"
/// - "ns1.%" → matches all tables in namespace "ns1"
/// - "ns1.ns2.table%" → matches tables in namespace "ns1.ns2"
pub(crate) fn parse_qualified_pattern(pattern: &str) -> QualifiedPattern<'_> {
    if let Some((namespace, table)) = pattern.rsplit_once('.') {
        QualifiedPattern {
            namespace: Some(namespace),
            table,
        }
    } else {
        QualifiedPattern {
            namespace: None,
            table: pattern,
        }
    }
}

/// Matches a SQL `LIKE` pattern against a value.
pub(crate) fn match_pattern(value: &str, pattern: &str) -> bool {
    like_pattern_to_regex(pattern)
        .and_then(|re| Regex::new(&re).ok())
        .map(|re| re.is_match(value))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::match_pattern;

    #[test]
    fn test_exact_match() {
        assert!(match_pattern("hello", "hello"));
        assert!(!match_pattern("hello", "world"));
    }

    #[test]
    fn test_percent_wildcard() {
        // % matches zero or more characters
        assert!(match_pattern("hello", "%"));
        assert!(match_pattern("hello", "h%"));
        assert!(match_pattern("hello", "%o"));
        assert!(match_pattern("hello", "he%o"));
        assert!(match_pattern("hello", "%ell%"));
        assert!(!match_pattern("hello", "x%"));
        assert!(!match_pattern("hello", "%x"));
    }

    #[test]
    fn test_underscore_wildcard() {
        // _ matches exactly one character
        assert!(match_pattern("hello", "h_llo"));
        assert!(match_pattern("hello", "he___"));
        assert!(match_pattern("hello", "_ello"));
        assert!(match_pattern("hello", "hell_"));
        assert!(match_pattern("hello", "h__lo"));
        assert!(!match_pattern("hello", "h_lo"));
    }

    #[test]
    fn test_escape_characters() {
        // \ escapes special characters
        assert!(match_pattern("hello%", "hello\\%"));
        assert!(match_pattern("hello_", "hello\\_"));
        assert!(match_pattern("hello\\", "hello\\\\"));
        assert!(!match_pattern("hello%", "hello\\_"));
    }

    #[test]
    fn test_empty_strings() {
        assert!(match_pattern("", ""));
        assert!(match_pattern("", "%"));
        assert!(!match_pattern("", "_"));
        assert!(!match_pattern("hello", ""));
    }

    #[test]
    fn test_mixed_wildcards() {
        assert!(match_pattern("hello", "h%o"));
        assert!(match_pattern("hello", "h_%o"));
        assert!(match_pattern("hello", "%_%"));
        assert!(match_pattern("hello", "_%_"));
    }

    #[test]
    fn test_value_exhausted_pattern_remaining() {
        assert!(match_pattern("abc", "abc%"));
        assert!(match_pattern("abc", "abc%%"));
        assert!(!match_pattern("abc", "abc_"));
        assert!(!match_pattern("abc", "abc%def"));
        assert!(!match_pattern("abc", "abc\\%"));
    }

    #[test]
    fn test_parse_qualified_pattern_unqualified() {
        let result = super::parse_qualified_pattern("table*");
        assert_eq!(
            result,
            super::QualifiedPattern {
                namespace: None,
                table: "table*"
            }
        );
    }

    #[test]
    fn test_parse_qualified_pattern_qualified() {
        let result = super::parse_qualified_pattern("ns1.table*");
        assert_eq!(
            result,
            super::QualifiedPattern {
                namespace: Some("ns1"),
                table: "table*"
            }
        );
    }

    #[test]
    fn test_parse_qualified_pattern_wildcard_tables() {
        let result = super::parse_qualified_pattern("ns1.*");
        assert_eq!(
            result,
            super::QualifiedPattern {
                namespace: Some("ns1"),
                table: "*"
            }
        );
    }

    #[test]
    fn test_parse_qualified_pattern_nested_namespace() {
        // For multi-level namespaces, split on last dot
        let result = super::parse_qualified_pattern("ns1.ns2.table*");
        assert_eq!(
            result,
            super::QualifiedPattern {
                namespace: Some("ns1.ns2"),
                table: "table*"
            }
        );
    }
}
