use async_stream::stream;
use futures::stream::{BoxStream, StreamExt};
use itertools::Itertools;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::Sender;

use globset::{GlobBuilder, GlobMatcher};
use lazy_static::lazy_static;

use crate::object_io::{FileMetadata, FileType, ObjectSource};

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
    pub fanout_limit: Option<usize>,
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
///   4. Preserves any leading delimiters
pub(crate) fn to_glob_fragments(glob_str: &str) -> super::Result<Vec<GlobFragment>> {
    println!("To glob fragments: {glob_str}");
    let delimiter = "/";

    // NOTE: We only use the URL parse library to get the scheme, because it will escape some of our glob special characters
    // such as ? and {}
    let glob_url = url::Url::parse(glob_str).map_err(|e| super::Error::InvalidUrl {
        path: glob_str.to_string(),
        source: e,
    })?;
    let url_scheme = glob_url.scheme();

    let glob_str_after_scheme = &glob_str[url_scheme.len() + SCHEME_SUFFIX_LEN..];

    // NOTE: Leading delimiter may be important for absolute paths on local directory, and is considered
    // part of the first fragment
    let leading_delimiter = if glob_str_after_scheme.starts_with(delimiter) {
        delimiter
    } else {
        ""
    };

    // Parse glob fragments: split by delimiter and join any non-special fragments
    let mut coalesced_fragments = vec![];
    let mut nonspecial_fragments_so_far = vec![];
    for fragment in glob_str_after_scheme
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

    // Ensure that the first fragment has the scheme and leading delimiter (if requested) prefixed
    coalesced_fragments[0] = GlobFragment::new(
        (format!("{url_scheme}://") + leading_delimiter + coalesced_fragments[0].raw_str())
            .as_str(),
    );

    println!("LEADING DELIM: {leading_delimiter}");

    Ok(coalesced_fragments)
}

/// Helper method to iterate on a directory with the following behavior
///
/// * First attempts to non-recursively list all Files and Directories under the current `uri`
/// * If during iteration we detect the number of Directories being returned exceeds `max_dirs`, we
///     fall back onto a prefix list of all Files with the current `uri` as the prefix
///
/// Returns a tuple `(file_metadata_stream: BoxStream<...>, dir_count: usize)` where the second element
/// indicates the number of Directory entries contained within the stream
async fn ls_with_prefix_fallback(
    source: Arc<dyn ObjectSource>,
    uri: &str,
    delimiter: &str,
    max_dirs: Option<usize>,
    page_size: Option<i32>,
) -> (BoxStream<'static, super::Result<FileMetadata>>, usize) {
    // Prefix list function that only returns Files
    fn prefix_ls(
        source: Arc<dyn ObjectSource>,
        path: String,
        delimiter: String,
        page_size: Option<i32>,
    ) -> BoxStream<'static, super::Result<FileMetadata>> {
        stream! {
            match source.iter_dir(&path, delimiter.as_str(), false, page_size).await {
                Ok(mut result_stream) => {
                    while let Some(result) = result_stream.next().await {
                        match result {
                            Ok(fm) => {
                                if matches!(fm.filetype, FileType::File)
                                {
                                    yield Ok(fm)
                                }
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                },
                Err(e) => yield Err(e),
            }
        }
        .boxed()
    }

    // Buffer results in memory as we go along
    let mut results_buffer = vec![];

    let mut fm_stream = source
        .iter_dir(uri, delimiter, true, page_size)
        .await
        .unwrap_or_else(|e| futures::stream::iter([Err(e)]).boxed());

    // Iterate and collect results into the `results_buffer`, but terminate early if too many directories are found
    let mut dir_count_so_far = 0;
    while let Some(fm) = fm_stream.next().await {
        if let Ok(fm) = &fm {
            if matches!(fm.filetype, FileType::Directory) {
                dir_count_so_far += 1;
                // STOP EARLY!!
                // If the number of directory results are more than `max_dirs`, we terminate the function early,
                // throw away our results buffer and return a stream of FileType::File files using `prefix_ls` instead
                if max_dirs
                    .map(|max_dirs| dir_count_so_far > max_dirs)
                    .unwrap_or(false)
                {
                    return (
                        prefix_ls(
                            source.clone(),
                            uri.to_string(),
                            delimiter.to_string(),
                            page_size,
                        ),
                        0,
                    );
                }
            }
        }
        results_buffer.push(fm);
    }

    // No early termination: we unwrap the results in our results buffer and yield data as a stream
    let s = futures::stream::iter(results_buffer);
    (s.boxed(), dir_count_so_far)
}

/// Globs an ObjectSource for Files
///
/// Uses the `globset` crate for matching, and thus supports all the syntax enabled by that crate.
/// See: https://docs.rs/globset/latest/globset/#syntax
///
/// Arguments:
/// * source: the ObjectSource to use for file listing
/// * glob: the string to glob
/// * fanout_limit: number of directories at which to fallback onto prefix listing, or None to never fall back.
///     A reasonable number here for a remote object store is something like 1024, which saturates the number of
///     parallel connections (usually defaulting to 64).
/// * page_size: control the returned results page size, or None to use the ObjectSource's defaults. Usually only used for testing
///     but may yield some performance improvements depending on the workload.
pub(crate) async fn glob(
    source: Arc<dyn ObjectSource>,
    glob: &str,
    fanout_limit: Option<usize>,
    page_size: Option<i32>,
) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
    // If no special characters, we fall back to ls behavior
    let full_fragment = GlobFragment::new(glob);
    if !full_fragment.has_special_character() {
        let glob = full_fragment.escaped_str().to_string();
        return Ok(stream! {
            let mut results = source.iter_dir(glob.as_str(), "/", true, page_size).await?;
            while let Some(result) = results.next().await {
                match result {
                    Ok(fm) => {
                        if matches!(fm.filetype, FileType::File) {
                            yield Ok(fm)
                        }
                    },
                    Err(e) => yield Err(e),
                }
            }
        }
        .boxed());
    }

    // If user specifies a trailing / then we understand it as an attempt to list the folder(s) matched
    // and append a trailing * fragment
    let glob = if glob.ends_with('/') {
        glob.to_string() + "*"
    } else {
        glob.to_string()
    };
    let glob = glob.as_str();

    let glob_fragments = to_glob_fragments(glob)?;
    let full_glob_matcher = GlobBuilder::new(glob)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map_err(|err| super::Error::InvalidArgument {
            msg: format!("Cannot parse provided glob {glob}: {err}"),
        })?
        .compile_matcher();

    // Channel to send results back to caller. Note that all results must have FileType::File.
    let (to_rtn_tx, mut to_rtn_rx) = tokio::sync::mpsc::channel(16 * 1024);

    /// Dispatches a task to visit the specified `path` (a concrete path on the filesystem to either a File or Directory).
    /// Based on the current glob_fragment being processed (accessible via `glob_fragments[i]`) this task will:
    ///   1. Perform work to retrieve Files/Directories at (`path` + `glob_fragments[i]`)
    ///   2. Return results to the provided `result_tx` channel based on the provided glob, if appropriate
    ///   3. Dispatch additional tasks via `.visit()` to continue visiting them, if appropriate
    fn visit(
        result_tx: Sender<super::Result<FileMetadata>>,
        source: Arc<dyn ObjectSource>,
        state: GlobState,
    ) {
        tokio::spawn(async move {
            println!(
                "Visiting '{}' with glob_fragments: {:?}",
                &state.current_path, &state.glob_fragments
            );
            let current_fragment = state.current_glob_fragment();

            // BASE CASE: current_fragment is a **
            // We perform a recursive ls and filter on the results for only FileType::File results that match the full glob
            if current_fragment.escaped_str() == "**" {
                let (mut results, stream_dir_count) = ls_with_prefix_fallback(
                    source.clone(),
                    &state.current_path,
                    "/",
                    state
                        .fanout_limit
                        .map(|fanout_limit| fanout_limit / state.current_fanout),
                    state.page_size,
                )
                .await;

                while let Some(val) = results.next().await {
                    match val {
                        Ok(fm) => {
                            match fm.filetype {
                                // Recursively visit each sub-directory
                                FileType::Directory => {
                                    visit(
                                        result_tx.clone(),
                                        source.clone(),
                                        // Do not increment `current_fragment_idx` so as to keep visiting the "**" fragmemt
                                        state.clone().advance(
                                            fm.filepath.clone(),
                                            state.current_fragment_idx,
                                            stream_dir_count,
                                        ),
                                    );
                                }
                                // Return any Files that match
                                FileType::File
                                    if state.full_glob_matcher.is_match(fm.filepath.as_str()) =>
                                {
                                    result_tx.send(Ok(fm)).await.expect("Internal multithreading channel is broken: results may be incorrect");
                                }
                                _ => (),
                            }
                        }
                        // Silence NotFound errors when in wildcard "search" mode
                        Err(super::Error::NotFound { .. }) if state.wildcard_mode => (),
                        Err(e) => result_tx.send(Err(e)).await.expect(
                            "Internal multithreading channel is broken: results may be incorrect",
                        ),
                    }
                }
            // BASE CASE: current fragment is the last fragment in `glob_fragments`
            } else if state.current_fragment_idx == state.glob_fragments.len() - 1 {
                // Last fragment contains a wildcard: we list the last level and match against the full glob
                if current_fragment.has_special_character() {
                    let mut results = source
                        .iter_dir(&state.current_path, "/", true, state.page_size)
                        .await
                        .unwrap_or_else(|e| futures::stream::iter([Err(e)]).boxed());

                    while let Some(result) = results.next().await {
                        match result {
                            Ok(fm) => {
                                if matches!(fm.filetype, FileType::File)
                                    && state.full_glob_matcher.is_match(fm.filepath.as_str())
                                {
                                    result_tx.send(Ok(fm)).await.expect("Internal multithreading channel is broken: results may be incorrect");
                                }
                            }
                            // Silence NotFound errors when in wildcard "search" mode
                            Err(super::Error::NotFound { .. }) if state.wildcard_mode => (),
                            Err(e) => result_tx.send(Err(e)).await.expect(
                                "Internal multithreading channel is broken: results may be incorrect",
                            ),
                        }
                    }
                // Last fragment does not contain wildcard: we return it if the full path exists and is a FileType::File
                } else {
                    let full_dir_path = state.current_path.clone() + current_fragment.escaped_str();
                    let single_file_ls = source
                        .ls(full_dir_path.as_str(), "/", true, None, state.page_size)
                        .await;
                    match single_file_ls {
                        Ok(mut single_file_ls) => {
                            if single_file_ls.files.len() == 1
                                && matches!(single_file_ls.files[0].filetype, FileType::File)
                            {
                                let fm = single_file_ls.files.drain(..).next().unwrap();
                                result_tx.send(Ok(fm)).await.expect("Internal multithreading channel is broken: results may be incorrect");
                            }
                        }
                        // Silence NotFound errors when in wildcard "search" mode
                        Err(super::Error::NotFound { .. }) if state.wildcard_mode => (),
                        Err(e) => result_tx.send(Err(e)).await.expect(
                            "Internal multithreading channel is broken: results may be incorrect",
                        ),
                    };
                }

            // RECURSIVE CASE: current_fragment contains a special character (e.g. *)
            } else if current_fragment.has_special_character() {
                let partial_glob_matcher = GlobBuilder::new(
                    GlobFragment::join(
                        &state.glob_fragments[..state.current_fragment_idx + 1],
                        "/",
                    )
                    .raw_str(),
                )
                .literal_separator(true)
                .build()
                .expect("Cannot parse glob")
                .compile_matcher();

                let (mut results, stream_dir_count) = ls_with_prefix_fallback(
                    source.clone(),
                    &state.current_path,
                    "/",
                    state
                        .fanout_limit
                        .map(|fanout_limit| fanout_limit / state.current_fanout),
                    state.page_size,
                )
                .await;

                while let Some(val) = results.next().await {
                    match val {
                        Ok(fm) => match fm.filetype {
                            FileType::Directory
                                if partial_glob_matcher
                                    .is_match(fm.filepath.as_str().trim_end_matches('/')) =>
                            {
                                visit(
                                    result_tx.clone(),
                                    source.clone(),
                                    state
                                        .clone()
                                        .advance(
                                            fm.filepath,
                                            state.current_fragment_idx + 1,
                                            stream_dir_count,
                                        )
                                        .with_wildcard_mode(),
                                );
                            }
                            FileType::File
                                if state.full_glob_matcher.is_match(fm.filepath.as_str()) =>
                            {
                                result_tx.send(Ok(fm)).await.expect("Internal multithreading channel is broken: results may be incorrect");
                            }
                            _ => (),
                        },
                        // Always silence NotFound since we are in wildcard "search" mode here by definition
                        Err(super::Error::NotFound { .. }) => (),
                        Err(e) => result_tx.send(Err(e)).await.expect(
                            "Internal multithreading channel is broken: results may be incorrect",
                        ),
                    }
                }

            // RECURSIVE CASE: current_fragment contains no special characters, and is a path to a specific File or Directory
            } else {
                let full_dir_path = state.current_path.clone() + current_fragment.escaped_str();
                visit(
                    result_tx.clone(),
                    source.clone(),
                    state
                        .clone()
                        .advance(full_dir_path, state.current_fragment_idx + 1, 1),
                );
            }
        });
    }

    visit(
        to_rtn_tx,
        source.clone(),
        GlobState {
            current_path: "".to_string(),
            current_fragment_idx: 0,
            glob_fragments: Arc::new(glob_fragments),
            full_glob_matcher: Arc::new(full_glob_matcher),
            wildcard_mode: false,
            current_fanout: 1,
            fanout_limit,
            page_size,
        },
    );

    let to_rtn_stream = stream! {
        while let Some(v) = to_rtn_rx.recv().await {
            yield v
        }
    };

    Ok(to_rtn_stream.boxed())
}
