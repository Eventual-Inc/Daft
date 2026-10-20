mod error;
mod lexer;

lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, clippy::pedantic, clippy::nursery)]
    grammar
);

use daft_ast::{Expr, Statement};
pub use error::ParseError;

/// Parses one SQL statement, optionally followed by semicolons.
pub fn parse(input: &str) -> Result<Statement, ParseError> {
    grammar::ProgramParser::new()
        .parse(lexer::Lexer::new(input))
        .map_err(|err| ParseError::from_lalrpop(err, input))
}

pub fn parse_expr(input: &str) -> Result<Expr, ParseError> {
    grammar::ExprParser::new()
        .parse(lexer::Lexer::new(input))
        .map_err(|err| ParseError::from_lalrpop(err, input))
}

#[cfg(test)]
mod tests {
    use daft_ast::Lit;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("1 + 2 * 3", "(1 + (2 * 3))")]
    #[case("a - b - c", "((a - b) - c)")]
    #[case("a / b % c", "((a / b) % c)")]
    #[case("-a * b", "((-a) * b)")]
    #[case("-(1 + 2)", "(-(1 + 2))")]
    #[case("NOT a = b AND c OR d", "(((NOT (a = b)) AND c) OR d)")]
    #[case("a OR b AND c", "(a OR (b AND c))")]
    #[case("NOT NOT false", "(NOT (NOT FALSE))")]
    #[case("a != b", "(a <> b)")]
    #[case("x >= 1.5 and y <= 1e3", "((x >= 1.5) AND (y <= 1000.0))")]
    #[case("TrUe Or null", "(TRUE OR NULL)")]
    #[case("s = 'it''s'", "(s = 'it''s')")]
    #[case("_col1 < .5", "(_col1 < 0.5)")]
    #[case("a == b", "(a = b)")]
    #[case("7 // 2 + +1", "((7 // 2) + (+1))")]
    #[case("-l[-1]::INT", "(-CAST((l[(-1)]) AS INT))")]
    #[case("a NOT IN (1) AND b", "((a NOT IN (1)) AND b)")]
    #[case("NOT a BETWEEN 1 AND 2 AND b", "((NOT (a BETWEEN 1 AND 2)) AND b)")]
    #[case("a + 1 NOT LIKE 'x' IS NULL", "(((a + 1) NOT LIKE 'x') IS NULL)")]
    #[case("b NOT NULL", "(b IS NOT NULL)")]
    #[case("t.s.g = \"X y\"", "(t.s.g = \"X y\")")]
    fn test_parse_display(#[case] input: &str, #[case] expected: &str) {
        let expr = parse_expr(input).unwrap();
        assert_eq!(expr.to_string(), expected);
        assert_eq!(
            parse_expr(expected).unwrap(),
            expr,
            "display must round-trip"
        );
    }

    #[test]
    fn test_parse_literals() {
        assert_eq!(
            parse_expr("'it''s'").unwrap(),
            Expr::Lit(Lit::String("it's".to_string()))
        );
        assert_eq!(parse_expr("42").unwrap(), Expr::Lit(Lit::Int(42)));
        assert_eq!(parse_expr("1.").unwrap(), Expr::Lit(Lit::Float(1.0)));
        assert_eq!(
            parse_expr("99999999999999999999").unwrap(),
            Expr::Lit(Lit::Float(1e20))
        );
        assert_eq!(parse_expr("NULL").unwrap(), Expr::Lit(Lit::Null));
        assert_eq!(parse_expr("1_000").unwrap(), Expr::Lit(Lit::Int(1000)));
        assert_eq!(
            parse_expr("'a'\n'b'\n'c'").unwrap(),
            Expr::Lit(Lit::String("abc".to_string()))
        );
    }

    #[rstest]
    #[case("a < b < c", 6..7)]
    #[case("a = b IN (1)", 6..8)]
    #[case("a IN (1) = b", 9..10)]
    #[case("x[1][2]", 4..5)]
    #[case("1 +", 3..3)]
    #[case("(1", 2..2)]
    #[case("1 2", 2..3)]
    #[case("1 # 2", 2..3)]
    #[case("'unterminated", 0..13)]
    #[case("'a' 'b'", 4..7)]
    #[case("1.2.3", 0..5)]
    #[case("1 !=- 1", 2..5)]
    #[case("`a` = 1", 0..3)]
    fn test_parse_error_span(#[case] input: &str, #[case] span: std::ops::Range<usize>) {
        assert_eq!(parse_expr(input).unwrap_err().span, span);
    }

    #[test]
    fn test_parse_integer_slot() {
        let err = parse("SELECT a FROM t LIMIT 1.5").unwrap_err();
        assert_eq!(
            err,
            ParseError::new("expected a non-negative integer, found \"1.5\"", 22..25)
        );
    }
}
