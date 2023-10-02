use itertools::Itertools;
use std::{collections::HashSet, sync::Arc};

use globset::GlobMatcher;
use lazy_static::lazy_static;

lazy_static! {
    /// Check if a given char is considered a special glob character
    /// NOTE: we use the `globset` crate which defines the following glob behavior:
    /// https://docs.rs/globset/latest/globset/index.html#syntax
    static ref GLOB_SPECIAL_CHARACTERS: HashSet<char> = HashSet::from(['*', '?', '{', '}', '[', ']']);
}

const SCHEME_SUFFIX_LEN: usize = "://".len();

#[derive(Clone)]
pub(crate) struct GlobState {
    // Current path in dirtree and glob_fragments
    pub current_path: String,
    pub current_fragment_idx: usize,

    // How large of a fanout this level of iteration is currently experiencing
    pub current_fanout: usize,

    // Whether we have encountered wildcards yet in the process of parsing
    pub wildcard_mode: bool,

    // Carry along expensive data as Arcs to avoid recomputation
    pub glob_fragments: Arc<Vec<GlobFragment>>,
    pub full_glob_matcher: Arc<GlobMatcher>,
    pub fanout_limit: usize,
    pub page_size: Option<i32>,
}

impl GlobState {
    pub fn current_glob_fragment(&self) -> &GlobFragment {
        &self.glob_fragments[self.current_fragment_idx]
    }

    pub fn advance(self, path: String, idx: usize, fanout_factor: usize) -> Self {
        GlobState {
            current_path: path,
            current_fragment_idx: idx,
            current_fanout: self.current_fanout * fanout_factor,
            ..self.clone()
        }
    }

    pub fn with_wildcard_mode(self) -> Self {
        GlobState {
            wildcard_mode: true,
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GlobFragment {
    data: String,
    escaped_data: String,
    first_wildcard_idx: Option<usize>,
}

impl GlobFragment {
    pub fn new(data: &str) -> Self {
        let first_wildcard_idx = match data {
            "" => None,
            data if GLOB_SPECIAL_CHARACTERS.contains(&data.chars().nth(0).unwrap()) => Some(0),
            _ => {
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
            }
        };

        // Sanitize `data`: removing '\' and converting '\\' to '\'
        let mut escaped_data = String::new();
        let mut ptr = 0;
        while ptr < data.len() {
            let remaining = &data[ptr..];
            match remaining.find(r"\\") {
                Some(backslash_idx) => {
                    escaped_data.push_str(&remaining[..backslash_idx].replace('\\', ""));
                    escaped_data.extend(std::iter::once('\\'));
                    ptr += backslash_idx + 2;
                }
                None => {
                    escaped_data.push_str(&remaining.replace('\\', ""));
                    break;
                }
            }
        }

        GlobFragment {
            data: data.to_string(),
            first_wildcard_idx,
            escaped_data,
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
                .join(sep)
                .as_str(),
        )
    }

    /// Returns the fragment as a string with the backslash (\) escapes applied
    /// 1. \\ is cleaned up to just \
    /// 2. \ followed by anything else is just ignored
    pub fn escaped_str(&self) -> &str {
        self.escaped_data.as_str()
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
pub(crate) fn to_glob_fragments(glob_str: &str) -> super::Result<Vec<GlobFragment>> {
    let delimiter = "/";

    // NOTE: We only use the URL parse library to get the scheme, because it will escape some of our glob special characters
    // such as ? and {}
    let glob_url = url::Url::parse(glob_str).map_err(|e| super::Error::InvalidUrl {
        path: glob_str.to_string(),
        source: e,
    })?;
    let url_scheme = glob_url.scheme();

    // Parse glob fragments: split by delimiter and join any non-special fragments
    let mut coalesced_fragments = vec![];
    let mut nonspecial_fragments_so_far = vec![];
    for fragment in glob_str[url_scheme.len() + SCHEME_SUFFIX_LEN..]
        .split(delimiter)
        .map(GlobFragment::new)
    {
        match fragment {
            fragment if fragment.data.is_empty() => (),
            fragment if fragment.has_special_character() => {
                if !nonspecial_fragments_so_far.is_empty() {
                    coalesced_fragments.push(GlobFragment::join(
                        nonspecial_fragments_so_far.drain(..).as_slice(),
                        delimiter,
                    ));
                }
                coalesced_fragments.push(fragment);
            }
            _ => {
                nonspecial_fragments_so_far.push(fragment);
            }
        }
    }
    if !nonspecial_fragments_so_far.is_empty() {
        coalesced_fragments.push(GlobFragment::join(
            nonspecial_fragments_so_far.drain(..).as_slice(),
            delimiter,
        ));
    }

    // Ensure that the first fragment has the scheme prefixed
    coalesced_fragments[0] =
        GlobFragment::new((format!("{url_scheme}://") + coalesced_fragments[0].raw_str()).as_str());

    Ok(coalesced_fragments)
}
