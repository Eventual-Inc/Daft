use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use globset::GlobBuilder;
use tokio::sync::mpsc::Sender;
use tokio::sync::OwnedSemaphorePermit;

use crate::glob::GlobState;
use crate::{
    glob::{to_glob_fragments, GlobFragment},
    local::{collect_file, LocalFile},
};

/// Default limit before we fallback onto parallel prefix list streams
static DEFAULT_FANOUT_LIMIT: usize = 1024;

pub enum GetResult {
    File(LocalFile),
    Stream(
        BoxStream<'static, super::Result<Bytes>>,
        Option<usize>,
        Option<OwnedSemaphorePermit>,
    ),
}

async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> super::Result<Bytes>
where
    S: Stream<Item = super::Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()?.unwrap_or_default();
    // Avoid copying if single response
    match stream.next().await.transpose()? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}

impl GetResult {
    pub async fn bytes(self) -> super::Result<Bytes> {
        use GetResult::*;
        match self {
            File(f) => collect_file(f).await,
            Stream(stream, size, _permit) => collect_bytes(stream, size).await,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    File,
    Directory,
}

impl TryFrom<std::fs::FileType> for FileType {
    type Error = DaftError;

    fn try_from(value: std::fs::FileType) -> Result<Self, Self::Error> {
        if value.is_dir() {
            Ok(Self::Directory)
        } else if value.is_file() {
            Ok(Self::File)
        } else if value.is_symlink() {
            Err(DaftError::InternalError(format!("Symlinks should never be encountered when constructing FileMetadata, but got: {:?}", value)))
        } else {
            unreachable!(
                "Can only be a directory, file, or symlink, but got: {:?}",
                value
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub filepath: String,
    pub size: Option<u64>,
    pub filetype: FileType,
}
#[derive(Debug)]
pub struct LSResult {
    pub files: Vec<FileMetadata>,
    pub continuation_token: Option<String>,
}

use async_stream::stream;

#[async_trait]
pub(crate) trait ObjectSource: Sync + Send {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult>;
    async fn get_range(&self, uri: &str, range: Range<usize>) -> super::Result<GetResult> {
        self.get(uri, Some(range)).await
    }
    async fn get_size(&self, uri: &str) -> super::Result<usize>;

    async fn ls(
        &self,
        path: &str,
        delimiter: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> super::Result<LSResult>;

    async fn iter_dir(
        &self,
        uri: &str,
        delimiter: &str,
        posix: bool,
        page_size: Option<i32>,
        _limit: Option<usize>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let delimiter = delimiter.to_string();
        let s = stream! {
            let lsr = self.ls(&uri, delimiter.as_str(), posix, None, page_size).await?;
            for fm in lsr.files {
                yield Ok(fm);
            }

            let mut continuation_token = lsr.continuation_token.clone();
            while continuation_token.is_some() {
                let lsr = self.ls(&uri, delimiter.as_str(), posix, continuation_token.as_deref(), page_size).await?;
                continuation_token = lsr.continuation_token.clone();
                for fm in lsr.files {
                    yield Ok(fm);
                }
            }
        };
        Ok(s.boxed())
    }
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
    max_dirs: usize,
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
            match source.iter_dir(&path, delimiter.as_str(), false, page_size, None).await {
                Ok(mut result_stream) => {
                    while let Some(fm) = result_stream.next().await {
                        match fm {
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
        .iter_dir(uri, delimiter, true, page_size, None)
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
                if dir_count_so_far > max_dirs {
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

pub(crate) async fn glob(
    source: Arc<dyn ObjectSource>,
    glob: &str,
    fanout_limit: Option<usize>,
    page_size: Option<i32>,
) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
    // If no special characters, we fall back to ls behavior
    let full_fragment = GlobFragment::new(glob);
    if !full_fragment.has_special_character() {
        let glob = full_fragment.escaped_str().to_string();
        return Ok(stream! {
            let mut results = source.iter_dir(glob.as_str(), "/", true, page_size, None).await?;
            while let Some(val) = results.next().await {
                match &val {
                    // Ignore non-File results
                    Ok(fm) if !matches!(fm.filetype, FileType::File) => continue,
                    _ => yield val,
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

    let fanout_limit = fanout_limit.unwrap_or(DEFAULT_FANOUT_LIMIT);
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
            log::debug!(target: "glob", "Visiting '{}' with glob_fragments: {:?}", &state.current_path, &state.glob_fragments);
            let current_fragment = state.current_glob_fragment();

            // BASE CASE: current_fragment is a **
            // We perform a recursive ls and filter on the results for only FileType::File results that match the full glob
            if current_fragment.escaped_str() == "**" {
                let (mut results, stream_dir_count) = ls_with_prefix_fallback(
                    source.clone(),
                    &state.current_path,
                    "/",
                    state.fanout_limit / state.current_fanout,
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
                        .iter_dir(&state.current_path, "/", true, state.page_size, None)
                        .await
                        .unwrap_or_else(|e| futures::stream::iter([Err(e)]).boxed());

                    while let Some(val) = results.next().await {
                        match val {
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
                    state.fanout_limit / state.current_fanout,
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

pub(crate) async fn recursive_iter(
    source: Arc<dyn ObjectSource>,
    uri: &str,
) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
    log::debug!(target: "recursive_iter", "starting recursive_iter: with top level of: {uri}");
    let (to_rtn_tx, mut to_rtn_rx) = tokio::sync::mpsc::channel(16 * 1024);
    fn add_to_channel(
        source: Arc<dyn ObjectSource>,
        tx: Sender<super::Result<FileMetadata>>,
        dir: String,
    ) {
        log::debug!(target: "recursive_iter", "recursive_iter: spawning task to list: {dir}");
        let source = source.clone();
        tokio::spawn(async move {
            let s = source.iter_dir(&dir, "/", true, Some(1000), None).await;
            log::debug!(target: "recursive_iter", "started listing task for {dir}");
            let mut s = match s {
                Ok(s) => s,
                Err(e) => {
                    log::debug!(target: "recursive_iter", "Error occurred when listing {dir}\nerror:\n{e}");
                    tx.send(Err(e)).await.map_err(|se| {
                        super::Error::UnableToSendDataOverChannel { source: se.into() }
                    })?;
                    return super::Result::<_, super::Error>::Ok(());
                }
            };
            let tx = &tx;
            while let Some(tr) = s.next().await {
                if let Ok(ref tr) = tr && matches!(tr.filetype, FileType::Directory) {
                    add_to_channel(source.clone(), tx.clone(), tr.filepath.clone())
                }
                tx.send(tr)
                    .await
                    .map_err(|e| super::Error::UnableToSendDataOverChannel { source: e.into() })?;
            }
            log::debug!(target: "recursive_iter", "completed listing task for {dir}");
            super::Result::Ok(())
        });
    }

    add_to_channel(source, to_rtn_tx, uri.to_string());

    let to_rtn_stream = stream! {
        while let Some(v) = to_rtn_rx.recv().await {
            yield v
        }
    };

    Ok(to_rtn_stream.boxed())
}
