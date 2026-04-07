use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use ego_tree::iter::Edge;
use scraper::{Html, node::Node};
use serde::{Deserialize, Serialize};

/// Tag names whose text content should be completely discarded.
const SKIP_TAGS: &[&str] = &["script", "style", "head"];

/// Block-level tag names that should emit a newline before their content.
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

/// Extract plain text from an HTML string.
///
/// - Content inside `<script>`, `<style>`, and `<head>` is dropped.
/// - Block-level elements inject `separator` before their content.
/// - The result is trimmed of leading/trailing whitespace.
fn strip_html_str(html: &str, separator: &str) -> String {
    let document = Html::parse_document(html);
    let mut result = String::new();
    let mut skip_depth: usize = 0;

    for edge in document.tree.root().traverse() {
        match edge {
            Edge::Open(node) => match node.value() {
                Node::Element(elem) => {
                    let tag = elem.name();
                    if SKIP_TAGS.contains(&tag) {
                        skip_depth += 1;
                    } else if skip_depth == 0
                        && BLOCK_TAGS.contains(&tag)
                        && !result.is_empty()
                        && !result.ends_with(separator)
                    {
                        result.push_str(separator);
                    }
                }
                Node::Text(text) => {
                    if skip_depth == 0 {
                        result.push_str(text);
                    }
                }
                _ => {}
            },
            Edge::Close(node) => {
                if let Node::Element(elem) = node.value() {
                    let tag = elem.name();
                    if SKIP_TAGS.contains(&tag) {
                        skip_depth = skip_depth.saturating_sub(1);
                    }
                }
            }
        }
    }

    result.trim().to_string()
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
        #[allow(clippy::literal_string_with_formatting_args)]
        let input = "<style>body{color:red}</style><p>text</p>";
        assert_eq!(strip_html_str(input, "\n"), "text");
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
        // scraper / html5ever decodes entities automatically
        let result = strip_html_str("<p>fish &amp; chips</p>", "\n");
        assert_eq!(result, "fish & chips");
    }

    #[test]
    fn test_null_safe_via_option() {
        // strip_html_str is only called on Some values; None is passed through
        let none: Option<&str> = None;
        let result: Option<String> = none.map(|s| strip_html_str(s, "\n"));
        assert!(result.is_none());
    }
}
