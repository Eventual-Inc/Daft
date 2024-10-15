pub struct WindowedWords<'a> {
    s: &'a str,
    word_starts: Vec<usize>, // Vec of start indices for each word
    window_size: usize,
    current: usize, // Current starting word index for the window
}

impl<'a> WindowedWords<'a> {
    /// Creates a new `WindowedWords` iterator.
    ///
    /// # Arguments
    ///
    /// * `s` - The input string slice.
    /// * `window_size` - The number of words in each window.
    ///
    /// # Example
    ///
    /// ```
    /// let s = "The quick brown fox";
    /// let iter = WindowedWords::new(s, 2);
    /// ```
    pub fn new(s: &'a str, window_size: usize) -> Self {
        assert!(window_size > 0, "Window size must be greater than 0");

        if s.is_empty() {
            return WindowedWords {
                s,
                word_starts: vec![],
                window_size,
                current: 0,
            };
        }

        // assume first character is not whitespace
        let mut word_starts = vec![0];

        for (i, _) in s.match_indices(' ') {
            // assume character after whitespace is not whitespace
            word_starts.push(i + 1);
        }

        WindowedWords {
            s,
            word_starts,
            window_size,
            current: 0,
        }
    }
}

impl<'a> Iterator for WindowedWords<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.window_size == 0 {
            return None;
        }

        if self.current + self.window_size <= self.word_starts.len() {
            // Get the start of the current window
            let start = self.word_starts[self.current];
            // Get the end of the window: end of the last word in the window
            let end = if self.current + self.window_size < self.word_starts.len() {
                self.word_starts[self.current + self.window_size]
            } else {
                self.s.len()
            };
            self.current += 1;
            Some(self.s[start..end].trim_end())
        } else if self.current == 0
            && !self.word_starts.is_empty()
            && self.window_size > self.word_starts.len()
        {
            // Yield a window with all words if window_size exceeds the number of words
            let start = self.word_starts[0];
            let end = self.s.len();
            self.current += 1;
            Some(&self.s[start..end])
        } else {
            // No more windows to yield
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.window_size == 0 {
            return (0, Some(0));
        }

        if self.window_size > self.word_starts.len() {
            if self.word_starts.is_empty() {
                (0, Some(0))
            } else {
                (1, Some(1))
            }
        } else {
            let remaining = self
                .word_starts
                .len()
                .saturating_sub(self.current + self.window_size - 1);
            (remaining, Some(remaining))
        }
    }
}

impl<'a> ExactSizeIterator for WindowedWords<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windowed_words() {
        let s = "The quick brown fox jumps over the lazy dog";
        let iter = WindowedWords::new(s, 3);
        let result: Vec<&str> = iter.collect();

        assert_eq!(
            result,
            vec![
                "The quick brown",
                "quick brown fox",
                "brown fox jumps",
                "fox jumps over",
                "jumps over the",
                "over the lazy",
                "the lazy dog",
            ]
        );
    }

    #[test]
    fn test_fewer_words_than_window_size() {
        let s = "Hello world";
        let iter = WindowedWords::new(s, 3);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hello world"]);
    }

    #[test]
    fn test_empty_string() {
        let s = "";
        let iter = WindowedWords::new(s, 3);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, Vec::<&str>::new());
    }

    #[test]
    fn test_single_word() {
        let s = "Hello";
        let iter = WindowedWords::new(s, 3);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hello"]);
    }

    // currently not supported for performance. see assumptions.
    // #[test]
    // fn test_with_extra_whitespace() {
    //     let s = "  The   quick  brown   ";
    //     let iter = WindowedWords::new(s, 2);
    //     let result: Vec<&str> = iter.collect();
    //
    //     assert_eq!(result, vec!["The   quick", "quick  brown"]);
    // }

    #[test]
    fn test_large_window_size() {
        let s = "One two three";
        let iter = WindowedWords::new(s, 5);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["One two three"]);
    }

    // currently not supported for performance. see assumptions.
    // #[test]
    // fn test_multiple_spaces_between_words() {
    //     let s = "Hello    world  from  Rust";
    //     let iter = WindowedWords::new(s, 2);
    //     let result: Vec<&str> = iter.collect();
    //
    //     assert_eq!(result, vec!["Hello    world", "world  from", "from  Rust"]);
    // }

    #[test]
    #[should_panic(expected = "Window size must be greater than 0")]
    fn test_window_size_zero() {
        let s = "This should yield nothing";
        let iter = WindowedWords::new(s, 0);
        let _result: Vec<&str> = iter.collect();
    }

    #[test]
    fn test_exact_window_size() {
        let s = "One two three four";
        let iter = WindowedWords::new(s, 4);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["One two three four"]);
    }

    #[test]
    fn test_window_size_one() {
        let s = "Single word windows";
        let iter = WindowedWords::new(s, 1);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Single", "word", "windows"]);
    }

    #[test]
    fn test_window_size_one_with_trailing_whitespace_no_panic() {
        let s = "Single word windows ";
        let iter = WindowedWords::new(s, 1);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Single", "word", "windows", ""]);
    }

    #[test]
    fn test_utf8_words() {
        let s = "Hello 世界 Rust язык";
        let iter = WindowedWords::new(s, 2);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hello 世界", "世界 Rust", "Rust язык",]);
    }

    #[test]
    fn test_utf8_single_word() {
        let s = "こんにちは"; // "Hello" in Japanese
        let iter = WindowedWords::new(s, 2);
        let result: Vec<&str> = iter.collect();

        // Since there's only one word, even with window_size > number of words, it should yield the single word
        assert_eq!(result, vec!["こんにちは"]);
    }

    #[test]
    fn test_utf8_mixed_languages() {
        let s = "Café naïve façade Москва Москва";
        let iter = WindowedWords::new(s, 3);
        let result: Vec<&str> = iter.collect();

        assert_eq!(
            result,
            vec![
                "Café naïve façade",
                "naïve façade Москва",
                "façade Москва Москва",
            ]
        );
    }

    #[test]
    fn test_utf8_with_emojis() {
        let s = "Hello 🌍 Rust 🚀 язык 📝";
        let iter = WindowedWords::new(s, 2);
        let result: Vec<&str> = iter.collect();

        assert_eq!(
            result,
            vec!["Hello 🌍", "🌍 Rust", "Rust 🚀", "🚀 язык", "язык 📝",]
        );
    }

    #[test]
    fn test_utf8_large_window_size() {
        let s = "One 两三 四五 六七八 九十";
        let iter = WindowedWords::new(s, 4);
        let result: Vec<&str> = iter.collect();

        assert_eq!(
            result,
            vec!["One 两三 四五 六七八", "两三 四五 六七八 九十",]
        );
    }

    #[test]
    fn test_utf8_exact_window_size() {
        let s = "Hola 世界 Bonjour мир";
        let iter = WindowedWords::new(s, 4);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hola 世界 Bonjour мир"]);
    }

    #[test]
    fn test_utf8_window_size_one() {
        let s = "Hello 世界 Rust язык 🐱‍👤";
        let iter = WindowedWords::new(s, 1);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤"],);
    }

    #[test]
    fn test_utf8_trailing_whitespace() {
        let s = "Hello 世界 Rust язык 🐱‍👤 ";
        let iter = WindowedWords::new(s, 1);
        let result: Vec<&str> = iter.collect();

        // The last window is an empty string due to trailing space
        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤", ""],);
    }
}
