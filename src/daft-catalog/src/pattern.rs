pub(crate) fn match_pattern(value: &str, pattern: &str) -> bool {
    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    let mut value_idx = 0;
    let mut pattern_idx = 0;
    let mut backtrack_stack: Vec<(usize, usize)> = Vec::new();
    let mut escaped = false;

    loop {
        if value_idx < value_chars.len() && pattern_idx < pattern_chars.len() {
            let vc = value_chars[value_idx];
            let pc = pattern_chars[pattern_idx];

            if escaped {
                if pc == vc {
                    value_idx += 1;
                    pattern_idx += 1;
                    escaped = false;
                } else if !try_backtrack(
                    &mut backtrack_stack,
                    &mut value_idx,
                    &mut pattern_idx,
                    &mut escaped,
                ) {
                    return false;
                }
            } else if pc == '\\' {
                pattern_idx += 1;
                escaped = true;
            } else {
                match pc {
                    '_' => {
                        value_idx += 1;
                        pattern_idx += 1;
                    }
                    '%' => {
                        // Save current position for potential backtracking
                        backtrack_stack.push((value_idx, pattern_idx + 1));
                        pattern_idx += 1;
                    }
                    _ if pc == vc => {
                        value_idx += 1;
                        pattern_idx += 1;
                    }
                    _ => {
                        if !try_backtrack(
                            &mut backtrack_stack,
                            &mut value_idx,
                            &mut pattern_idx,
                            &mut escaped,
                        ) {
                            return false;
                        }
                    }
                }
            }
        } else if value_idx < value_chars.len() {
            // Pattern is exhausted. This is only valid if we have a '%' to backtrack to
            if !try_backtrack(
                &mut backtrack_stack,
                &mut value_idx,
                &mut pattern_idx,
                &mut escaped,
            ) {
                return false;
            }
        } else if pattern_idx < pattern_chars.len() {
            // Value is exhausted, check if the remaining pattern chars can match zero characters
            // This means all remaining chars must be '%' wildcards, accounting for escape sequences
            return can_match_zero_chars(&pattern_chars[pattern_idx..], escaped);
        } else {
            // Both exhausted - perfect match
            return true;
        }
    }
}

fn try_backtrack(
    backtrack_stack: &mut Vec<(usize, usize)>,
    value_idx: &mut usize,
    pattern_idx: &mut usize,
    escaped: &mut bool,
) -> bool {
    if let Some((backtrack_value_idx, backtrack_pattern_idx)) = backtrack_stack.pop() {
        // Reset to position after the last '%'
        *value_idx = backtrack_value_idx + 1;
        *pattern_idx = backtrack_pattern_idx;
        *escaped = false;
        backtrack_stack.push((*value_idx, *pattern_idx));
        true
    } else {
        false
    }
}

fn can_match_zero_chars(pattern_chars: &[char], mut escaped: bool) -> bool {
    let mut idx = 0;
    while idx < pattern_chars.len() {
        if escaped {
            // Escaped character means a literal match is required, so can't match zero characters.
            return false;
        }
        match pattern_chars[idx] {
            '\\' => {
                // Escape next character. If no character follows, it's invalid.
                idx += 1;
                if idx >= pattern_chars.len() {
                    return false;
                }
                escaped = true;
            }
            '%' => {
                // Wildcard may match zero characters.
                idx += 1;
            }
            _ => {
                // Any other character means a literal match is needed.
                return false;
            }
        }
    }
    // If we end with an outstanding escape, the sequence is incomplete.
    !escaped
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
}
