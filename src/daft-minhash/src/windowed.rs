use std::{
    iter::{Map, Once},
    str::MatchIndices,
};

pub trait WindowedWordsExt<'a> {
    fn windowed_words(&'a self, window_size: usize) -> impl Iterator<Item = &'a str>;
}

struct WindowedWords<'a> {
    first: bool,
    text: &'a str,
    word_boundaries: Vec<usize>,
    window_size: usize,
    current_idx: usize,
}

impl<'a> WindowedWords<'a> {
    fn new(text: &'a str, window_size: usize) -> Self {
        assert!(window_size > 0, "Window size must be greater than 0");

        let mut word_boundaries = Vec::new();

        if !text.is_empty() {
            // Add start position
            word_boundaries.push(0);
            // Add all space positions
            for elem in memchr::memchr_iter(b' ', text.as_bytes()) {
                word_boundaries.push(elem); // Position after space
            }
            // Add end position
            word_boundaries.push(text.len());
        }

        WindowedWords {
            first: true,
            text,
            word_boundaries,
            window_size,
            current_idx: 0,
        }
    }
}

impl<'a> Iterator for WindowedWords<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.text.is_empty() {
            return None;
        }
        let is_first = self.first;
        self.first = false;

        if self.current_idx + self.window_size >= self.word_boundaries.len() {
            if is_first && !self.text.is_empty() {
                return Some(self.text);
            }

            return None;
        }

        let start = self.word_boundaries[self.current_idx];
        let end = self.word_boundaries[self.current_idx + self.window_size];
        self.current_idx += 1;

        if is_first {
            Some(&self.text[(start)..end])
        } else {
            Some(&self.text[(start + 1)..end])
        }
    }
}

impl<'a> WindowedWordsExt<'a> for str {
    #[inline]
    fn windowed_words(&'a self, window_size: usize) -> impl Iterator<Item = &'a Self> {
        WindowedWords::new(self, window_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windowed_words() {
        let s = "The quick brown fox jumps over the lazy dog";
        let result: Vec<&str> = s.windowed_words(3).collect();

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
        let result: Vec<&str> = s.windowed_words(3).collect();

        assert_eq!(result, vec!["Hello world"]);
    }

    #[test]
    fn test_empty_string() {
        let s = "";
        let result: Vec<&str> = s.windowed_words(3).collect();

        assert_eq!(result, Vec::<&str>::new());
    }

    #[test]
    fn test_single_word() {
        let s = "Hello";
        let result: Vec<&str> = s.windowed_words(3).collect();

        assert_eq!(result, vec!["Hello"]);
    }

    // currently not supported for performance. see assumptions.
    // #[test]
    // fn test_with_extra_whitespace() {
    //     let s = "  The   quick  brown   ";
    //     let result: Vec<&str> = s.windowed_words(2).collect();
    //
    //     assert_eq!(result, vec!["The   quick", "quick  brown"]);
    // }

    #[test]
    fn test_large_window_size() {
        let s = "One two three";
        let result: Vec<&str> = s.windowed_words(5).collect();

        assert_eq!(result, vec!["One two three"]);
    }

    // currently not supported for performance. see assumptions.
    // #[test]
    // fn test_multiple_spaces_between_words() {
    //     let s = "Hello    world  from  Rust";
    //     let result: Vec<&str> = s.windowed_words(2).collect();
    //
    //     assert_eq!(result, vec!["Hello    world", "world  from", "from  Rust"]);
    // }

    #[test]
    #[should_panic(expected = "Window size must be greater than 0")]
    fn test_window_size_zero() {
        let s = "This should yield nothing";
        let _result: Vec<&str> = s.windowed_words(0).collect();
    }

    #[test]
    fn test_exact_window_size() {
        let s = "One two three four";
        let result: Vec<&str> = s.windowed_words(4).collect();

        assert_eq!(result, vec!["One two three four"]);
    }

    #[test]
    fn test_window_size_one() {
        let s = "Single word windows";
        let result: Vec<&str> = s.windowed_words(1).collect();

        assert_eq!(result, vec!["Single", "word", "windows"]);
    }

    #[test]
    fn test_window_size_one_with_trailing_whitespace_no_panic() {
        let s = "Single word windows ";
        let result: Vec<&str> = s.windowed_words(1).collect();

        assert_eq!(result, vec!["Single", "word", "windows", ""]);
    }

    #[test]
    fn test_utf8_words() {
        let s = "Hello 世界 Rust язык";
        let result: Vec<&str> = s.windowed_words(2).collect();

        assert_eq!(result, vec!["Hello 世界", "世界 Rust", "Rust язык",]);
    }

    #[test]
    fn test_utf8_single_word() {
        let s = "こんにちは"; // "Hello" in Japanese
        let result: Vec<&str> = s.windowed_words(2).collect();

        // Since there's only one word, even with window_size > number of words, it should yield the single word
        assert_eq!(result, vec!["こんにちは"]);
    }

    #[test]
    fn test_utf8_mixed_languages() {
        let s = "Café naïve façade Москва Москва";
        let result: Vec<&str> = s.windowed_words(3).collect();

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
        let result: Vec<&str> = s.windowed_words(2).collect();

        assert_eq!(
            result,
            vec!["Hello 🌍", "🌍 Rust", "Rust 🚀", "🚀 язык", "язык 📝",]
        );
    }

    #[test]
    fn test_utf8_large_window_size() {
        let s = "One 两三 四五 六七八 九十";
        let result: Vec<&str> = s.windowed_words(4).collect();

        assert_eq!(
            result,
            vec!["One 两三 四五 六七八", "两三 四五 六七八 九十",]
        );
    }

    #[test]
    fn test_utf8_exact_window_size() {
        let s = "Hola 世界 Bonjour мир";
        let result: Vec<&str> = s.windowed_words(4).collect();

        assert_eq!(result, vec!["Hola 世界 Bonjour мир"]);
    }

    #[test]
    fn test_utf8_window_size_one() {
        let s = "Hello 世界 Rust язык 🐱‍👤";
        let result: Vec<&str> = s.windowed_words(1).collect();

        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤"],);
    }

    #[test]
    fn test_utf8_trailing_whitespace() {
        let s = "Hello 世界 Rust язык 🐱‍👤 ";
        let result: Vec<&str> = s.windowed_words(1).collect();

        // The last window is an empty string due to trailing space
        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤", ""],);
    }
}
