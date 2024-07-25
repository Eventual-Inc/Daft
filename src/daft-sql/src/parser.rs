use sqlparser::{
    ast::{Expr, Statement},
    dialect::Dialect,
    parser::{Parser, ParserError},
    tokenizer::Tokenizer,
};

/// Daft SQL Parser based on [`sqlparser`] and [`datafusion-sql`].
///
/// Reference: https://github.com/sqlparser-rs/sqlparser-rs/blob/main/docs/custom_sql_parser.md
pub struct DaftParser<'a> {
    parser: Parser<'a>,
}

impl<'a> DaftParser<'a> {
    /// Create a new parser for the given SQL statement using the DaftDialect.
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &DaftDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(DaftParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse the given SQL statement and return the AST.
    pub fn parse(sql: &str) -> Result<Statement, ParserError> {
        let mut parser = Self::new(sql)?;
        parser.parser.parse_statement()
    }
}

/// Daft Dialect for [`sqlparser`].
#[derive(Debug)]
pub struct DaftDialect {}

impl Dialect for DaftDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    /// The default SQL "
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        None
    }

    fn is_proper_identifier_inside_quotes(
        &self,
        mut _chars: std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        true
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        false
    }

    fn supports_window_clause_named_window_reference(&self) -> bool {
        false
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        false
    }

    fn supports_group_by_expr(&self) -> bool {
        // TODO
        false
    }

    fn supports_connect_by(&self) -> bool {
        false
    }

    fn supports_match_recognize(&self) -> bool {
        false
    }

    fn supports_in_empty_list(&self) -> bool {
        false
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        false
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        false
    }

    fn supports_numeric_prefix(&self) -> bool {
        false
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        false
    }

    fn supports_dictionary_syntax(&self) -> bool {
        false
    }

    fn supports_lambda_functions(&self) -> bool {
        false
    }

    fn supports_parenthesized_set_variables(&self) -> bool {
        false
    }

    fn supports_select_wildcard_except(&self) -> bool {
        false
    }

    fn convert_type_before_value(&self) -> bool {
        false
    }

    fn supports_triple_quoted_string(&self) -> bool {
        false
    }

    fn parse_prefix(&self, _parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    fn parse_infix(
        &self,
        _parser: &mut Parser,
        _expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    fn get_next_precedence(&self, _parser: &Parser) -> Option<Result<u8, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    fn parse_statement(
        &self,
        _parser: &mut Parser,
    ) -> Option<Result<sqlparser::ast::Statement, ParserError>> {
        // return None to fall back to the default behavior
        None
    }
}
