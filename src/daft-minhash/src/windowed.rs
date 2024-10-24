use std::collections::VecDeque;

pub trait WindowedWordsExt<'a> {
    fn windowed_words_in(
        &'a self,
        window_size: usize,
        alloc: &'a mut VecDeque<isize>,
    ) -> impl Iterator<Item = &'a str>;
}

struct WindowedWords<'a> {
    text: &'a str,
    queue: &'a mut VecDeque<isize>,
    space_iter: memchr::Memchr<'a>,
    window_size: usize,
}

impl<'a> WindowedWords<'a> {
    fn new(text: &'a str, window_size: usize, queue: &'a mut VecDeque<isize>) -> Self {
        assert!(window_size > 0, "Window size must be greater than 0");

        queue.clear();
        queue.push_back(-1);

        let mut boundaries = memchr::memchr_iter(b' ', text.as_bytes());

        for _ in 0..window_size {
            if let Some(boundary) = boundaries.next() {
                queue.push_back(boundary as isize);
            }
        }

        WindowedWords {
            text,
            queue,
            space_iter: boundaries,
            window_size,
        }
    }
}

impl<'a> Iterator for WindowedWords<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.text.is_empty() {
            return None;
        }

        let start = self.queue.pop_front().unwrap();
        let start = unsafe { usize::try_from(start + 1).unwrap_unchecked() };

        if self.queue.len() < self.window_size {
            let text = self.text;
            self.text = "";
            return Some(&text[start..]);
        }

        let end = *self.queue.back().unwrap();
        let end = unsafe { usize::try_from(end).unwrap_unchecked() };

        if let Some(next_boundary) = self.space_iter.next() {
            let next_boundary = next_boundary as isize;
            self.queue.push_back(next_boundary);
        }

        Some(&self.text[start..end])
    }
}

impl<'a> WindowedWordsExt<'a> for str {
    #[inline]
    fn windowed_words_in(
        &'a self,
        window_size: usize,
        alloc: &'a mut VecDeque<isize>,
    ) -> impl Iterator<Item = &'a Self> {
        WindowedWords::new(self, window_size, alloc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windowed_words() {
        let s = "The quick brown fox jumps over the lazy dog";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(3, &mut alloc).collect();

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
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(3, &mut alloc).collect();

        assert_eq!(result, vec!["Hello world"]);
    }

    #[test]
    fn test_empty_string() {
        let s = "";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(3, &mut alloc).collect();

        assert_eq!(result, Vec::<&str>::new());
    }

    #[test]
    fn test_single_word() {
        let s = "Hello";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(3, &mut alloc).collect();

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
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(5, &mut alloc).collect();

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
        let mut alloc = VecDeque::new();
        let _result: Vec<&str> = s.windowed_words_in(0, &mut alloc).collect();
    }

    #[test]
    fn test_exact_window_size() {
        let s = "One two three four";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(4, &mut alloc).collect();

        assert_eq!(result, vec!["One two three four"]);
    }

    #[test]
    fn test_window_size_one() {
        let s = "Single word windows";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(1, &mut alloc).collect();

        assert_eq!(result, vec!["Single", "word", "windows"]);
    }

    #[test]
    fn test_window_size_one_with_trailing_whitespace_no_panic() {
        let s = "Single word windows ";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(1, &mut alloc).collect();

        assert_eq!(result, vec!["Single", "word", "windows", ""]);
    }

    #[test]
    fn test_utf8_words() {
        let s = "Hello 世界 Rust язык";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(2, &mut alloc).collect();

        assert_eq!(result, vec!["Hello 世界", "世界 Rust", "Rust язык",]);
    }

    #[test]
    fn test_utf8_single_word() {
        let s = "こんにちは"; // "Hello" in Japanese
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(2, &mut alloc).collect();

        // Since there's only one word, even with window_size > number of words, it should yield the single word
        assert_eq!(result, vec!["こんにちは"]);
    }

    #[test]
    fn test_utf8_mixed_languages() {
        let s = "Café naïve façade Москва Москва";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(3, &mut alloc).collect();

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
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(2, &mut alloc).collect();

        assert_eq!(
            result,
            vec!["Hello 🌍", "🌍 Rust", "Rust 🚀", "🚀 язык", "язык 📝",]
        );
    }

    #[test]
    fn test_utf8_large_window_size() {
        let s = "One 两三 四五 六七八 九十";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(4, &mut alloc).collect();

        assert_eq!(
            result,
            vec!["One 两三 四五 六七八", "两三 四五 六七八 九十",]
        );
    }

    #[test]
    fn test_utf8_exact_window_size() {
        let s = "Hola 世界 Bonjour мир";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(4, &mut alloc).collect();

        assert_eq!(result, vec!["Hola 世界 Bonjour мир"]);
    }

    #[test]
    fn test_utf8_window_size_one() {
        let s = "Hello 世界 Rust язык 🐱‍👤";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(1, &mut alloc).collect();

        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤"],);
    }

    #[test]
    fn test_utf8_trailing_whitespace() {
        let s = "Hello 世界 Rust язык 🐱‍👤 ";
        let mut alloc = VecDeque::new();
        let result: Vec<&str> = s.windowed_words_in(1, &mut alloc).collect();

        // The last window is an empty string due to trailing space
        assert_eq!(result, vec!["Hello", "世界", "Rust", "язык", "🐱‍👤", ""],);
    }
}
