/// Converts a SQL `LIKE` pattern into a regex string anchored at start and end.
///
/// Supports `%` (zero or more chars), `_` (single char) and `\` as an escape for
/// any following character (including `%`, `_`, and `\` itself).
pub fn like_pattern_to_regex(pattern: &str) -> Option<String> {
    let mut regex = String::with_capacity(pattern.len() + 2);
    regex.push('^');

    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => match chars.next() {
                Some(next_ch) => regex.push_str(&regex::escape(&next_ch.to_string())),
                None => return None,
            },
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '[' | ']' => regex.push(ch),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }

    regex.push('$');
    Some(regex)
}

#[cfg(test)]
mod tests {
    use super::like_pattern_to_regex;

    fn is_match(value: &str, pattern: &str) -> bool {
        match like_pattern_to_regex(pattern) {
            Some(re) => regex::Regex::new(&re)
                .map(|r| r.is_match(value))
                .unwrap_or(false),
            None => false,
        }
    }

    #[test]
    fn exact_match() {
        assert!(is_match("hello", "hello"));
        assert!(!is_match("hello", "world"));
    }

    #[test]
    fn percent_wildcard() {
        assert!(is_match("hello", "%"));
        assert!(is_match("hello", "h%"));
        assert!(is_match("hello", "%o"));
        assert!(is_match("hello", "he%o"));
        assert!(is_match("hello", "%ell%"));
        assert!(!is_match("hello", "x%"));
        assert!(!is_match("hello", "%x"));
    }

    #[test]
    fn underscore_wildcard() {
        assert!(is_match("hello", "h_llo"));
        assert!(is_match("hello", "he___"));
        assert!(is_match("hello", "_ello"));
        assert!(is_match("hello", "hell_"));
        assert!(is_match("hello", "h__lo"));
        assert!(!is_match("hello", "h_lo"));
    }

    #[test]
    fn escape_characters() {
        assert!(is_match("hello%", "hello\\%"));
        assert!(is_match("hello_", "hello\\_"));
        assert!(is_match("hello\\", "hello\\\\"));
        assert!(!is_match("hello%", "hello\\_"));
    }

    #[test]
    fn empty_strings() {
        assert!(is_match("", ""));
        assert!(is_match("", "%"));
        assert!(!is_match("", "_"));
        assert!(!is_match("hello", ""));
    }

    #[test]
    fn mixed_wildcards() {
        assert!(is_match("hello", "h%o"));
        assert!(is_match("hello", "h_%o"));
        assert!(is_match("hello", "%_%"));
        assert!(is_match("hello", "_%_"));
    }

    #[test]
    fn value_exhausted_pattern_remaining() {
        assert!(is_match("abc", "abc%"));
        assert!(is_match("abc", "abc%%"));
        assert!(!is_match("abc", "abc_"));
        assert!(!is_match("abc", "abc%def"));
        assert!(!is_match("abc", "abc\\%"));
    }

    #[test]
    fn trailing_escape_errors() {
        assert!(like_pattern_to_regex("abc\\").is_none());
    }
}
