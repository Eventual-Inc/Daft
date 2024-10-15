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
        // Precompute word start indices by iterating once through the string
        let mut word_starts = Vec::new();
        let mut in_word = false;

        for (i, c) in s.char_indices() {
            if !c.is_whitespace() {
                if !in_word {
                    in_word = true;
                    word_starts.push(i);
                }
            } else {
                in_word = false;
            }
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

    #[test]
    fn test_with_extra_whitespace() {
        let s = "  The   quick  brown   ";
        let iter = WindowedWords::new(s, 2);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["The   quick", "quick  brown"]);
    }

    #[test]
    fn test_large_window_size() {
        let s = "One two three";
        let iter = WindowedWords::new(s, 5);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["One two three"]);
    }

    #[test]
    fn test_multiple_spaces_between_words() {
        let s = "Hello    world  from  Rust";
        let iter = WindowedWords::new(s, 2);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, vec!["Hello    world", "world  from", "from  Rust"]);
    }

    #[test]
    fn test_window_size_zero() {
        let s = "This should yield nothing";
        let iter = WindowedWords::new(s, 0);
        let result: Vec<&str> = iter.collect();

        assert_eq!(result, Vec::<&str>::new());
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
}
