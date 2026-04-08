use std::cell::{Cell, RefCell};

use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use html5ever::{
    tendril::StrTendril,
    tokenizer::{
        BufferQueue, CharacterTokens, EndTag, StartTag, TagToken, Token, TokenSink,
        TokenSinkResult, Tokenizer, TokenizerOpts, states,
    },
};
use serde::{Deserialize, Serialize};

/// Tag names whose text content should be completely discarded.
/// `script` and `style` use the tokenizer's raw-data / script-data modes for
/// correct end-tag detection even when the content contains `</` sequences.
/// `head` is processed in normal mode but its content is still skipped.
const SKIP_TAGS: &[&str] = &["script", "style", "head"];

/// Block-level tag names that should emit the separator before their content.
const BLOCK_TAGS: &[&str] = &[
    "p",
    "div",
    "br",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "tr",
    "blockquote",
    "pre",
    "article",
    "section",
    "header",
    "footer",
    "main",
    "nav",
    "aside",
    "dl",
    "dt",
    "dd",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StripHtml;

#[derive(FunctionArgs)]
struct StripHtmlArgs<T> {
    input: T,
    #[arg(optional)]
    separator: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for StripHtml {
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let StripHtmlArgs { input, separator } = inputs.try_into()?;
        let sep = separator.as_deref().unwrap_or("\n");
        strip_html_impl(&input, sep)
    }

    fn name(&self) -> &'static str {
        "strip_html"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let StripHtmlArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_string(),
            TypeError: "strip_html requires a String column, got {}",
            field.dtype
        );
        Ok(field)
    }

    fn docstring(&self) -> &'static str {
        "Strips HTML tags from a string, returning plain text. Content inside <script> and \
         <style> tags is removed entirely. Block-level elements (<p>, <div>, <br>, <h1>-<h6>, \
         <li>, etc.) are separated by `separator` (default: newline)."
    }
}

// ---------------------------------------------------------------------------
// Tokenizer sink
// ---------------------------------------------------------------------------

/// Accumulates plain text while the html5ever tokenizer feeds us tokens.
///
/// `process_token` takes `&self`, so we use `Cell`/`RefCell` for interior
/// mutability — this is the standard pattern for html5ever sinks.
struct HtmlTextExtractor<'a> {
    separator: &'a str,
    result: RefCell<String>,
    skip_depth: Cell<usize>,
}

impl<'a> HtmlTextExtractor<'a> {
    fn new(separator: &'a str) -> Self {
        Self {
            separator,
            result: RefCell::new(String::new()),
            skip_depth: Cell::new(0),
        }
    }
}

impl TokenSink for HtmlTextExtractor<'_> {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
        match token {
            TagToken(tag) => {
                let name = tag.name.as_ref();
                match tag.kind {
                    StartTag => {
                        if SKIP_TAGS.contains(&name) {
                            self.skip_depth.set(self.skip_depth.get() + 1);
                            // Signal the tokenizer to enter the correct raw mode so
                            // that end-tag detection is spec-correct even for content
                            // that contains `</` sequences (e.g. JS strings in scripts).
                            // Note: Script(()) must NOT be used here — it pauses the
                            // tokenizer and returns early from feed(), so subsequent
                            // HTML would never be processed. RawData(ScriptData) gives
                            // identical tokenizer behaviour without the early return.
                            return TokenSinkResult::RawData(if name == "script" {
                                states::ScriptData
                            } else {
                                states::Rawtext
                            });
                        } else if self.skip_depth.get() == 0 {
                            let mut result = self.result.borrow_mut();
                            if BLOCK_TAGS.contains(&name)
                                && !result.is_empty()
                                && !result.ends_with(self.separator)
                            {
                                result.push_str(self.separator);
                            }
                        }
                    }
                    EndTag => {
                        if SKIP_TAGS.contains(&name) {
                            self.skip_depth.set(self.skip_depth.get().saturating_sub(1));
                        }
                    }
                }
            }
            CharacterTokens(s) => {
                if self.skip_depth.get() == 0 {
                    self.result.borrow_mut().push_str(&s);
                }
            }
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

// ---------------------------------------------------------------------------
// Core function
// ---------------------------------------------------------------------------

/// Extract plain text from an HTML string using the html5ever tokenizer.
///
/// - Content inside `<script>`, `<style>`, and `<head>` is dropped.
/// - Block-level elements inject `separator` before their content.
/// - The result is trimmed of leading/trailing whitespace.
fn strip_html_str(html: &str, separator: &str) -> String {
    let sink = HtmlTextExtractor::new(separator);
    let input = BufferQueue::default();
    input.push_back(StrTendril::from(html));
    let tok = Tokenizer::new(sink, TokenizerOpts::default());
    let _ = tok.feed(&input);
    tok.end();
    tok.sink.result.into_inner().trim().to_string()
}

fn strip_html_impl(input: &Series, separator: &str) -> DaftResult<Series> {
    input.with_utf8_array(|arr| {
        Ok(Utf8Array::from_iter(
            arr.name(),
            arr.into_iter()
                .map(|maybe_s| maybe_s.map(|s| strip_html_str(s, separator))),
        )
        .into_series())
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::strip_html_str;

    #[test]
    fn test_basic_tags() {
        assert_eq!(strip_html_str("<p>Hello world</p>", "\n"), "Hello world");
    }

    #[test]
    fn test_script_removed() {
        assert_eq!(
            strip_html_str("<p>text</p><script>alert(1)</script>", "\n"),
            "text"
        );
    }

    #[test]
    fn test_style_removed() {
        let input = "<style>body { color: red; }</style><p>text</p>";
        assert_eq!(strip_html_str(input, "\n"), "text");
    }

    #[test]
    fn test_script_with_closing_tag_in_string() {
        // Without raw/script-data mode the inner </script> would end the element
        // prematurely; the tokenizer must handle this correctly.
        let input = r#"<p>ok</p><script>var s = "<\/script>";</script><p>after</p>"#;
        let result = strip_html_str(input, "\n");
        assert!(result.contains("ok"), "got: {result:?}");
        assert!(result.contains("after"), "got: {result:?}");
        assert!(!result.contains("var"), "got: {result:?}");
    }

    #[test]
    fn test_block_newlines() {
        let result = strip_html_str("<p>first</p><p>second</p>", "\n");
        assert!(
            result.contains('\n'),
            "block elements should produce newlines: {result:?}"
        );
    }

    #[test]
    fn test_block_space_separator() {
        let result = strip_html_str("<p>first</p><p>second</p>", " ");
        assert_eq!(result, "first second");
    }

    #[test]
    fn test_block_empty_separator() {
        let result = strip_html_str("<p>first</p><p>second</p>", "");
        assert_eq!(result, "firstsecond");
    }

    #[test]
    fn test_entities_decoded() {
        let result = strip_html_str("<p>fish &amp; chips</p>", "\n");
        assert_eq!(result, "fish & chips");
    }

    #[test]
    fn test_null_safe_via_option() {
        let none: Option<&str> = None;
        let result: Option<String> = none.map(|s| strip_html_str(s, "\n"));
        assert!(result.is_none());
    }
}
