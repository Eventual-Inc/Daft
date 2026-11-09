use std::fmt::Write;

use snafu::CleanedErrorText;

/// Formats an error and its full `source()` chain for display to users (e.g. Python exceptions).
///
/// Uses snafu's [`CleanedErrorText`] to deduplicate overlapping text between nested errors,
/// which is common with AWS SDK and other layered error types.
pub(crate) fn format_error_for_user(err: &dyn std::error::Error) -> String {
    let mut messages: Vec<String> = CleanedErrorText::new(err)
        .map(|(_, msg, _)| msg)
        .filter(|msg| !msg.trim().is_empty())
        .collect();

    // After deduplication, thiserror wrappers like `DaftError::External {inner}` often collapse
    // to a bare variant prefix with no remaining detail. Drop those so the first line is useful.
    while messages.len() > 1 && is_bare_error_variant_prefix(&messages[0]) {
        messages.remove(0);
    }

    // `CleanedErrorText` strips the source's text out of a wrapper's own message, which can leave a
    // dangling connector such as `Due to`. Rejoin those with the message they introduce so the
    // chain reads as one line instead of a "Caused by" list with an empty lead-in.
    let messages = merge_dangling_connectors(messages);

    match messages.len() {
        0 => String::new(),
        1 => messages[0].clone(),
        _ => {
            let mut out = messages[0].clone();
            out.push_str("\n\nCaused by the following errors:");
            for (i, msg) in messages.iter().skip(1).enumerate() {
                write!(out, "\n  {}: {msg}", i + 1).expect("Failed to write to string");
            }
            out
        }
    }
}

fn merge_dangling_connectors(messages: Vec<String>) -> Vec<String> {
    let mut merged: Vec<String> = Vec::with_capacity(messages.len());
    for msg in messages {
        match merged.last_mut() {
            Some(prev) if prev.trim_end().ends_with("Due to") => {
                let trimmed_len = prev.trim_end().len();
                prev.truncate(trimmed_len);
                prev.push_str(": ");
                prev.push_str(&msg);
            }
            _ => merged.push(msg),
        }
    }
    merged
}

fn is_bare_error_variant_prefix(message: &str) -> bool {
    let message = message.trim();
    if let Some(rest) = message.strip_prefix("DaftError::") {
        return !rest.contains(' ');
    }
    false
}

#[cfg(test)]
mod tests {
    use snafu::Snafu;

    use super::format_error_for_user;
    use crate::DaftError;

    #[derive(Debug, Snafu)]
    enum CreationError {
        #[snafu(display("Unable to create logical plan node.\nDue to: {}", source))]
        Creation { source: DaftError },
    }

    #[test]
    fn nested_external_error_is_flattened() {
        let inner = DaftError::ValueError(
            "Attempting to make a Schema with duplicate field names: count".to_string(),
        );
        let err = DaftError::External(Box::new(CreationError::Creation { source: inner }));
        let formatted = format_error_for_user(&err);
        assert_eq!(
            formatted,
            "Unable to create logical plan node.\nDue to: DaftError::ValueError Attempting to make a Schema with duplicate field names: count"
        );
    }
}
