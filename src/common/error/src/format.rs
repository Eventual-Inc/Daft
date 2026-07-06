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

fn is_bare_error_variant_prefix(message: &str) -> bool {
    let message = message.trim();
    if let Some(rest) = message.strip_prefix("DaftError::") {
        return !rest.contains(' ');
    }
    false
}
