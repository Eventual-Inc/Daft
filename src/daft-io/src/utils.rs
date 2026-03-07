use std::collections::HashMap;

use globset::GlobBuilder;
use itertools::Itertools;
use url::{ParseError, Position};

use crate::Error;

#[derive(Debug)]
pub struct ObjectPath {
    pub scheme: String,
    pub bucket: String,
    pub key: String,
}

/// NOTE: Our globbing logic makes very strong assumptions about the delimiter being used to denote
/// directories. The concept of a "glob" is a Unix concept anyways, and so even for Windows machines
/// the `glob` utility can only be used with POSIX-style paths.
pub(crate) const GLOB_DELIMITER: &str = "/";

/// Parse the scheme, bucket and key from the object url.
pub fn parse_object_url(uri: &str) -> super::Result<ObjectPath> {
    let parsed = url::Url::parse(uri).map_err(|source| Error::InvalidUrl {
        path: uri.into(),
        source,
    })?;

    let bucket = match parsed.host_str() {
        Some(s) => Ok(s),
        None => Err(Error::InvalidUrl {
            path: uri.into(),
            source: ParseError::EmptyHost,
        }),
    }?;

    // Use raw `uri` for object key: URI special character escaping might mangle key
    let bucket_scheme_prefix_len = parsed[..Position::AfterHost].len();
    let key = uri[bucket_scheme_prefix_len..].trim_start_matches(GLOB_DELIMITER);

    Ok(ObjectPath {
        scheme: parsed.scheme().to_string(),
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

/// Group the input glob paths according to whether there are overlapping
pub fn group_glob_paths(glob_paths: &[String]) -> super::Result<Vec<Vec<String>>> {
    if glob_paths.is_empty() {
        return Ok(vec![]);
    }

    let glob_paths = glob_paths.iter().unique().cloned().collect::<Vec<_>>();
    let compiled_glob_paths = glob_paths
        .iter()
        .map(|path| {
            // If user specifies a trailing / then we understand it as an attempt to list the folder(s) matched
            // and append a trailing * fragment
            let glob = if path.ends_with(GLOB_DELIMITER) {
                path.clone() + "*"
            } else {
                path.clone()
            };
            let glob = glob.as_str();

            // Validate the glob pattern, this is necessary since the `globset` crate is overly
            // permissive and happily compiles patterns like "/foo/bar/**.txt" which don't make sense.
            match verify_glob(glob) {
                Ok(..) => GlobBuilder::new(glob)
                    .literal_separator(true)
                    .backslash_escape(true)
                    .build()
                    .map_err(|err| Error::InvalidArgument {
                        msg: format!("Cannot parse provided glob {glob}: {err}"),
                    }),
                Err(err) => Err(Error::InvalidArgument {
                    msg: format!("Invalid provided glob {glob}: {err}"),
                }),
            }
        })
        .collect::<super::Result<Vec<_>>>()?;

    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while root != parent[root] {
            root = parent[root];
        }
        let mut curr = i;
        while curr != root {
            let next = parent[curr];
            parent[curr] = root;
            curr = next;
        }
        root
    }

    fn union(parent: &mut [usize], i: usize, j: usize) {
        let root_i = find(parent, i);
        let root_j = find(parent, j);
        if root_i != root_j {
            parent[root_i] = root_j;
        }
    }

    let n = glob_paths.len();
    let mut parent: Vec<usize> = (0..n).collect();
    for i in 0..n {
        for j in (i + 1)..n {
            let is_match = |compiled_glob_path: &globset::Glob, path: &str| {
                compiled_glob_path.compile_matcher().is_match(path)
            };

            let is_parent = |parent: &str, child: &str| {
                if parent.ends_with(GLOB_DELIMITER) {
                    child.starts_with(parent)
                } else {
                    child.starts_with(&format!("{}{}", parent, GLOB_DELIMITER))
                }
            };

            let overlap = is_match(&compiled_glob_paths[i], &glob_paths[j])
                || is_match(&compiled_glob_paths[j], &glob_paths[i])
                || is_parent(&glob_paths[i], &glob_paths[j])
                || is_parent(&glob_paths[j], &glob_paths[i]);

            if overlap {
                union(&mut parent, i, j);
            }
        }
    }

    let mut groups_map: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, glob) in glob_paths.iter().enumerate() {
        let root = find(&mut parent, i);
        groups_map.entry(root).or_default().push(glob.clone());
    }

    Ok(groups_map.into_values().collect())
}

/// Validates the glob pattern before compiling it. The `globset` crate which we use for globbing is
/// very permissive and does not check for invalid usage of the '**' wildcard. This function ensures
/// that the glob pattern does not contain invalid usage of '**'.
pub(crate) fn verify_glob(glob: &str) -> super::Result<()> {
    let re = regex::Regex::new(r"(?P<before>.*?[^\\])\*\*(?P<after>[^/\n].*)").unwrap();

    if let Some(captures) = re.captures(glob) {
        let before = captures.name("before").map_or("", |m| m.as_str());
        let after = captures.name("after").map_or("", |m| m.as_str());

        // Ensure the 'before' part ends with a delimiter
        let corrected_before = if !before.ends_with(GLOB_DELIMITER) {
            format!("{}{}", before, GLOB_DELIMITER)
        } else {
            before.to_string()
        };

        let corrected_pattern = format!("{corrected_before}**/*{after}");
        return Err(Error::InvalidArgument {
            msg: format!(
                "Invalid usage of '**' in glob pattern. Found '{before}**{after}'. \
                The '**' wildcard should be used to match directories and must be surrounded by delimiters. \
                Did you perhaps mean: '{corrected_pattern}'?"
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::utils::{group_glob_paths, verify_glob};

    #[test]
    fn test_verify_glob() {
        // Test valid glob patterns
        assert!(verify_glob("valid/pattern.txt").is_ok()); // Normal globbing works ok
        assert!(verify_glob("another/valid/pattern/**/blah.txt").is_ok()); // No error if ** used as a segment
        assert!(verify_glob("**").is_ok()); // ** by itself is ok
        assert!(verify_glob("another/valid/pattern/**").is_ok()); // No trailing slash is ok
        assert!(verify_glob("another/valid/pattern/**/").is_ok()); // Trailing slash is ok (should be interpreted as **/*)
        assert!(verify_glob("another/valid/pattern/**/\\**.txt").is_ok()); // Escaped ** is ok
        assert!(verify_glob("**/wildcard/*.txt").is_ok()); // Wildcard matching not affected

        // Test invalid glob patterns and check error messages
        // The '**' wildcard should be used to match directories and must be surrounded by delimiters.
        let err = verify_glob("invalid/**.txt").unwrap_err();
        assert!(err.to_string().contains("invalid/**/*.txt")); // Suggests adding a delimiter after '**'

        // '**' should be surrounded by delimiters to match directories, not used directly with file names.
        let err = verify_glob("invalid/blahblah**.txt").unwrap_err();
        assert!(err.to_string().contains("invalid/blahblah/**/*.txt")); // Suggests adding a delimiter before '**'

        // Backslash should only escape the first '*', leading to non-escaped '**'.
        let err = verify_glob("invalid/\\***.txt").unwrap_err();
        assert!(err.to_string().contains("invalid/\\\\*/**/*.txt")); // Suggests correcting the escape sequence (NOTE: double backslash)

        // Non-escaped '**' should trigger even when there is an escaped '**'.
        let err = verify_glob("invalid/\\**blahblah**.txt").unwrap_err();
        assert!(err.to_string().contains("invalid/\\\\**blahblah/**/*.txt")); // Suggests adding delimiters around '**'
    }

    #[test]
    fn test_group_glob_paths() {
        let groups = group_glob_paths(&vec![
            "/data1/2026/**/*.parquet".to_string(),
            "s3://ai/data/2026/**/[x,y,z].json".to_string(),
            "/data1/2026/01/*.parquet".to_string(),
            "/data1/2025/[a,b].parquet".to_string(),
            "s3://ai/data/*.csv".to_string(),
            "/data1/2026/01/a.parquet".to_string(),
            "/data1/2026/02/10/b.parquet".to_string(),
            "s3://ai/data/2026/02/x.json".to_string(),
            "s3://ai/data/2026/02/1?/z.json".to_string(),
            "/data1/2025/*.parquet".to_string(),
        ])
        .unwrap();

        let mut groups: Vec<Vec<String>> = groups
            .into_iter()
            .map(|mut group| {
                group.sort();
                group
            })
            .collect();
        groups.sort_by_key(|group| group.len());

        assert_eq!(4, groups.len());
        assert_eq!(
            vec![
                vec!["s3://ai/data/*.csv".to_string()],
                vec![
                    "/data1/2025/*.parquet".to_string(),
                    "/data1/2025/[a,b].parquet".to_string(),
                ],
                vec![
                    "s3://ai/data/2026/**/[x,y,z].json".to_string(),
                    "s3://ai/data/2026/02/1?/z.json".to_string(),
                    "s3://ai/data/2026/02/x.json".to_string(),
                ],
                vec![
                    "/data1/2026/**/*.parquet".to_string(),
                    "/data1/2026/01/*.parquet".to_string(),
                    "/data1/2026/01/a.parquet".to_string(),
                    "/data1/2026/02/10/b.parquet".to_string(),
                ],
            ],
            groups
        );
    }

    #[test]
    fn test_group_no_overlapping_glob_paths() {
        let mut groups = group_glob_paths(&vec![
            "tos://ai/data/a.csv".to_string(),
            "s3://ai/data/a.csv".to_string(),
            "tos://ai/data/[x,y,z].csv".to_string(),
        ])
        .unwrap();
        groups.sort();

        assert_eq!(3, groups.len());
        assert_eq!(
            vec![
                vec!["s3://ai/data/a.csv".to_string()],
                vec!["tos://ai/data/[x,y,z].csv".to_string()],
                vec!["tos://ai/data/a.csv".to_string()],
            ],
            groups
        );
    }

    #[test]
    fn test_group_all_overlapping_glob_paths() {
        let mut groups = group_glob_paths(&vec![
            "/data/**/*".to_string(),
            "/data/[x,y,z].csv".to_string(),
            "/data/test?/*.parquet".to_string(),
        ])
        .unwrap();
        groups.sort();

        assert_eq!(1, groups.len());
        assert_eq!(
            vec![
                "/data/**/*".to_string(),
                "/data/[x,y,z].csv".to_string(),
                "/data/test?/*.parquet".to_string(),
            ],
            groups[0]
        );
    }
}
