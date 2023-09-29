use std::collections::HashSet;

use lazy_static::lazy_static;
use url::Position;

lazy_static! {
    /// Check if a given char is considered a special glob character
    /// NOTE: we use the `globset` crate which defines the following glob behavior:
    /// https://docs.rs/globset/latest/globset/index.html#syntax
    static ref GLOB_SPECIAL_CHARACTERS: HashSet<char> = {
        let mut set = HashSet::new();
        set.insert('*');
        set.insert('?');
        set.insert('{');
        set.insert('[');
        set
    };
}

#[derive(Debug, Clone)]
pub(crate) struct GlobFragment {
    data: String,
    first_wildcard_idx: Option<usize>,
}

impl GlobFragment {
    pub fn new(data: &str) -> Self {
        let first_wildcard_idx = if data.is_empty() {
            None
        } else if GLOB_SPECIAL_CHARACTERS.contains(&data.chars().nth(0).unwrap()) {
            Some(0)
        } else {
            // Detect any special characters that are not preceded by an escape \
            let mut idx = None;
            for (i, window) in data
                .chars()
                .collect::<Vec<char>>()
                .as_slice()
                .windows(2)
                .enumerate()
            {
                let &[c1, c2] = window else {
                    unreachable!("Window contains 2 elements")
                };
                if (c1 != '\\') && GLOB_SPECIAL_CHARACTERS.contains(&c2) {
                    idx = Some(i + 1);
                    break;
                }
            }
            idx
        };
        GlobFragment {
            data: data.to_string(),
            first_wildcard_idx,
        }
    }

    /// Checks if this GlobFragment has any special characters
    pub fn has_special_character(&self) -> bool {
        self.first_wildcard_idx.is_some()
    }

    /// Joins a slice of GlobFragments together with a separator
    pub fn join(fragments: &[GlobFragment], sep: &str) -> Self {
        GlobFragment::new(
            fragments
                .iter()
                .map(|frag: &GlobFragment| frag.data.as_str())
                .collect::<Vec<&str>>()
                .join(sep)
                .as_str(),
        )
    }

    /// Returns the fragment as a string with the backslash (\) escapes applied
    /// 1. \\ is cleaned up to just \
    /// 2. \ followed by anything else is just ignored
    pub fn escaped_str(&self) -> String {
        let mut result = String::new();
        let mut ptr = 0;
        while ptr < self.data.len() {
            let remaining = &self.data.as_str()[ptr..];
            match remaining.find("\\\\") {
                Some(backslash_idx) => {
                    result.push_str(&remaining[..backslash_idx].replace('\\', ""));
                    result.extend(std::iter::once('\\'));
                    ptr += backslash_idx + 2;
                }
                None => {
                    result.push_str(&remaining.replace('\\', ""));
                    break;
                }
            }
        }
        result
    }

    /// Returns the GlobFragment as a raw unescaped string, suitable for use by the globset crate
    pub fn raw_str(&self) -> &str {
        self.data.as_str()
    }
}

/// Parses a glob URL string into "fragments"
/// Fragments are the glob URL string but:
///   1. Split by delimiter ("/")
///   2. Non-wildcard fragments are joined and coalesced by delimiter
///   3. The first fragment is prefixed by "{scheme}://"
pub(crate) fn to_glob_fragments(glob_str: &str) -> Vec<GlobFragment> {
    let delimiter = "/".to_string();
    let glob_url = url::Url::parse(glob_str)
        .unwrap_or_else(|_| panic!("Glob string must be able to be parsed as URL: {glob_str}"));
    let url_scheme = glob_url.scheme();

    // Parse glob fragments: split by delimiter and join any non-wildcard fragments
    let mut glob_fragments = glob_url[Position::BeforeUsername..].split(&delimiter).fold(
        (vec![], vec![]),
        |(mut acc, mut fragments_so_far), current_fragment| {
            let current_fragment = GlobFragment::new(current_fragment);
            if current_fragment.has_special_character() {
                if !fragments_so_far.is_empty() {
                    acc.push(GlobFragment::join(
                        fragments_so_far.as_slice(),
                        delimiter.as_str(),
                    ));
                }
                acc.push(current_fragment);
                (acc, vec![])
            } else {
                fragments_so_far.push(current_fragment);
                (acc, fragments_so_far)
            }
        },
    );
    let mut glob_fragments = if glob_fragments.1.is_empty() {
        glob_fragments.0
    } else {
        let last_fragment = GlobFragment::join(glob_fragments.1.as_slice(), delimiter.as_str());
        glob_fragments
            .0
            .drain(..)
            .chain(std::iter::once(last_fragment))
            .collect()
    };
    glob_fragments[0] =
        GlobFragment::new((format!("{url_scheme}://") + glob_fragments[0].raw_str()).as_str());

    glob_fragments
}
