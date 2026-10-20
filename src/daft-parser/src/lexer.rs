use logos::{Logos, Skip, SpannedIter};

use crate::error::ParseError;

#[derive(Debug, Clone, Default, PartialEq, Eq, thiserror::Error)]
pub enum LexError {
    /// Logos requires a default; unreachable because every byte starts some token.
    #[default]
    #[error("unexpected character")]
    UnexpectedChar,
    #[error("unterminated string literal")]
    UnterminatedString,
    #[error("unterminated quoted identifier")]
    UnterminatedQuotedIdent,
    #[error("zero-length delimited identifier")]
    ZeroLengthIdent,
    #[error("unterminated /* comment at or near \"{0}\"")]
    UnterminatedComment(String),
    #[error("unterminated dollar-quoted string")]
    UnterminatedDollarString,
}

/// Literal and name variants carry raw source text, quotes and prefixes included.
#[derive(Logos, Debug, Clone, PartialEq, Eq)]
#[logos(error = LexError)]
#[logos(extras = Extras)]
// Unicode spaces separate tokens just like ASCII ones.
#[logos(subpattern space = r"[ \t\n\x0B\x0C\r\u{A0}\u{2000}-\u{200B}\u{202F}\u{205F}\u{2060}\u{3000}\u{FEFF}]")]
// Anything but space, digits, and ASCII punctuation other than `_`.
#[logos(subpattern word_start = r"[A-Za-z_\x00-\x08\x0E-\x1F\x7F\u{80}-\u{9F}\u{A1}-\u{1FFF}\u{200C}-\u{202E}\u{2030}-\u{205E}\u{2061}-\u{2FFF}\u{3001}-\u{FEFE}\u{FF00}-\u{10FFFF}]")]
#[logos(skip r"(?&space)+")]
#[logos(skip(r"--[^\n\r]*", allow_greedy = true))]
#[logos(skip(r"/\*", block_comment))]
pub enum Token<'input> {
    // Punctuation
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token(",")]
    Comma,
    #[token(";")]
    Semicolon,
    #[token("?")]
    Question,
    #[token("#")]
    Hash,
    #[token(":", colon)]
    Colon,
    DoubleColon,
    #[token("$", dollar)]
    Dollar,
    /// A lone `.`; `number` also starts here and returns `Number` for anything longer.
    #[regex(r"[0-9.]", number)]
    Dot,
    /// A whole operator run such as `<=`, `->>`, or `!=-`; the grammar matches its text.
    #[regex(r"[!%&*+\-/<=>@\\^|~]", operator_run)]
    Operator(&'input str),

    // Keywords
    #[token("all", ignore(case))]
    All,
    #[token("and", ignore(case))]
    And,
    #[token("array", ignore(case))]
    Array,
    #[token("as", ignore(case))]
    As,
    #[token("asc", ignore(case))]
    Asc,
    #[token("bernoulli", ignore(case))]
    Bernoulli,
    #[token("between", ignore(case))]
    Between,
    #[token("by", ignore(case))]
    By,
    #[token("case", ignore(case))]
    Case,
    #[token("cast", ignore(case))]
    Cast,
    #[token("coalesce", ignore(case))]
    Coalesce,
    #[token("create", ignore(case))]
    Create,
    #[token("current", ignore(case))]
    Current,
    #[token("desc", ignore(case))]
    Desc,
    #[token("describe", ignore(case))]
    Describe,
    #[token("distinct", ignore(case))]
    Distinct,
    #[token("else", ignore(case))]
    Else,
    #[token("end", ignore(case))]
    End,
    #[token("exclude", ignore(case))]
    Exclude,
    #[token("exists", ignore(case))]
    Exists,
    #[token("extract", ignore(case))]
    Extract,
    #[token("first", ignore(case))]
    First,
    #[token("following", ignore(case))]
    Following,
    #[token("for", ignore(case))]
    For,
    #[token("from", ignore(case))]
    From,
    #[token("full", ignore(case))]
    Full,
    #[token("group", ignore(case))]
    Group,
    #[token("having", ignore(case))]
    Having,
    #[token("if", ignore(case))]
    If,
    #[token("ilike", ignore(case))]
    ILike,
    #[token("in", ignore(case))]
    In,
    #[token("inner", ignore(case))]
    Inner,
    #[token("intersect", ignore(case))]
    Intersect,
    #[token("interval", ignore(case))]
    Interval,
    #[token("is", ignore(case))]
    Is,
    #[token("join", ignore(case))]
    Join,
    #[token("last", ignore(case))]
    Last,
    #[token("left", ignore(case))]
    Left,
    #[token("like", ignore(case))]
    Like,
    #[token("limit", ignore(case))]
    Limit,
    #[token("name", ignore(case))]
    Name,
    #[token("not", ignore(case))]
    Not,
    #[token("nulls", ignore(case))]
    Nulls,
    #[token("offset", ignore(case))]
    Offset,
    #[token("on", ignore(case))]
    On,
    #[token("or", ignore(case))]
    Or,
    #[token("order", ignore(case))]
    Order,
    #[token("outer", ignore(case))]
    Outer,
    #[token("over", ignore(case))]
    Over,
    #[token("partition", ignore(case))]
    Partition,
    #[token("percent", ignore(case))]
    Percent,
    #[token("preceding", ignore(case))]
    Preceding,
    #[token("precision", ignore(case))]
    Precision,
    #[token("range", ignore(case))]
    Range,
    #[token("repeatable", ignore(case))]
    Repeatable,
    #[token("right", ignore(case))]
    Right,
    #[token("rollup", ignore(case))]
    Rollup,
    #[token("row", ignore(case))]
    Row,
    #[token("rows", ignore(case))]
    Rows,
    #[token("select", ignore(case))]
    Select,
    #[token("show", ignore(case))]
    Show,
    #[token("substring", ignore(case))]
    Substring,
    #[token("system", ignore(case))]
    System,
    #[token("table", ignore(case))]
    Table,
    #[token("tables", ignore(case))]
    Tables,
    #[token("tablesample", ignore(case))]
    Tablesample,
    #[token("then", ignore(case))]
    Then,
    #[token("try_cast", ignore(case))]
    TryCast,
    #[token("unbounded", ignore(case))]
    Unbounded,
    #[token("union", ignore(case))]
    Union,
    #[token("use", ignore(case))]
    Use,
    #[token("using", ignore(case))]
    Using,
    #[token("when", ignore(case))]
    When,
    #[token("where", ignore(case))]
    Where,
    #[token("with", ignore(case))]
    With,

    // Never in the grammar, so DuckDB's `t SEMI JOIN u` is an error, not a table aliased `semi`.
    #[token("anti", ignore(case))]
    #[token("asof", ignore(case))]
    #[token("semi", ignore(case))]
    Reserved,

    // Date parts
    #[token("year", ignore(case))]
    Year,
    #[token("quarter", ignore(case))]
    Quarter,
    #[token("month", ignore(case))]
    Month,
    #[token("week", ignore(case))]
    Week,
    #[token("day", ignore(case))]
    Day,
    #[token("hour", ignore(case))]
    Hour,
    #[token("minute", ignore(case))]
    Minute,
    #[token("second", ignore(case))]
    Second,
    #[token("millisecond", ignore(case))]
    Millisecond,
    #[token("microsecond", ignore(case))]
    Microsecond,

    // Types
    #[token("bigint", ignore(case))]
    TypeBigint,
    #[token("binary", ignore(case))]
    TypeBinary,
    #[token("bool", ignore(case))]
    TypeBool,
    #[token("boolean", ignore(case))]
    TypeBoolean,
    #[token("date", ignore(case))]
    TypeDate,
    #[token("dec", ignore(case))]
    TypeDec,
    #[token("decimal", ignore(case))]
    TypeDecimal,
    #[token("double", ignore(case))]
    TypeDouble,
    #[token("float", ignore(case))]
    TypeFloat,
    #[token("int", ignore(case))]
    TypeInt,
    #[token("int16", ignore(case))]
    TypeInt16,
    #[token("int32", ignore(case))]
    TypeInt32,
    #[token("int64", ignore(case))]
    TypeInt64,
    #[token("integer", ignore(case))]
    TypeInteger,
    #[token("numeric", ignore(case))]
    TypeNumeric,
    #[token("real", ignore(case))]
    TypeReal,
    #[token("smallint", ignore(case))]
    TypeSmallint,
    #[token("string", ignore(case))]
    TypeString,
    #[token("text", ignore(case))]
    TypeText,
    #[token("time", ignore(case))]
    TypeTime,
    #[token("timestamp", ignore(case))]
    TypeTimestamp,
    #[token("tinyint", ignore(case))]
    TypeTinyint,
    #[token("uint16", ignore(case))]
    TypeUint16,
    #[token("uint32", ignore(case))]
    TypeUint32,
    #[token("uint64", ignore(case))]
    TypeUint64,
    #[token("varchar", ignore(case))]
    TypeVarchar,

    // Literals and names
    #[token("null", ignore(case))]
    Null,
    #[token("true", ignore(case))]
    True,
    #[token("false", ignore(case))]
    False,
    Number(&'input str),
    #[regex(r"'([^']|'')*'")]
    #[regex(r"'([^']|'')*", |_| Err(LexError::UnterminatedString))]
    String(&'input str),
    /// A `'…'` that may continue the string before it; see `Lexer::continues_string`.
    StringContinuation(&'input str),
    /// `N'…'`, `X'…'`, `B'…'`, or `E'…'`, where `\` escapes the next byte.
    #[regex(r"[NnXxBb]'([^']|'')*'")]
    #[regex(r"[NnXxBb]'([^']|'')*", |_| Err(LexError::UnterminatedString))]
    #[regex(r"[Ee]'([^'\\]|''|\\(?s:.))*'")]
    #[regex(r"[Ee]'([^'\\]|''|\\(?s:.))*\\?", |_| Err(LexError::UnterminatedString))]
    PrefixedString(&'input str),
    DollarString(&'input str),
    #[regex(r#""([^"]|"")*""#, |lex| quoted_ident(lex.slice()))]
    #[regex(r#""([^"]|"")*"#, |_| Err(LexError::UnterminatedQuotedIdent))]
    QuotedIdent(&'input str),
    #[regex(r"`([^`]|``)*`", |lex| quoted_ident(lex.slice()))]
    #[regex(r"`([^`]|``)*", |_| Err(LexError::UnterminatedQuotedIdent))]
    BacktickIdent(&'input str),
    #[regex(r"(?&word_start)((?&word_start)|[0-9$])*")]
    Ident(&'input str),
}

type Lex<'input> = logos::Lexer<'input, Token<'input>>;

#[derive(Default)]
pub struct Extras {
    /// Start of the last block comment, which blocks string continuation.
    block_comment: Option<usize>,
    /// Untrimmed end of the last operator run; a token starting before it is a dropped `+`/`-`.
    run_end: usize,
}

/// Bytes that continue an operator run.
fn is_operator_byte(b: u8) -> bool {
    b"+-*/%^<>=~!@&|".contains(&b)
}

/// Bumps over an operator run that stops before `--` or `/*`, then drops trailing `+`/`-` unless
/// the run holds a special byte. Dropped bytes re-lex as one-byte tokens.
fn bump_operator_run(lex: &mut Lex<'_>) {
    let src = lex.source().as_bytes();
    let start = lex.span().start;
    // A dropped byte is a whole token; rescanning the rest of its run would be quadratic.
    if start < lex.extras.run_end {
        return;
    }
    let mut end = start + 1;
    while end < src.len()
        && is_operator_byte(src[end])
        && !src[end..].starts_with(b"--")
        && !src[end..].starts_with(b"/*")
    {
        end += 1;
    }
    lex.extras.run_end = end;
    if !src[start..end].iter().any(|b| b"~!@#%^&|`?".contains(b)) {
        while end > start + 1 && matches!(src[end - 1], b'+' | b'-') {
            end -= 1;
        }
    }
    lex.bump(end - lex.span().end);
}

fn operator_run<'input>(lex: &mut Lex<'input>) -> &'input str {
    bump_operator_run(lex);
    lex.slice()
}

/// `::` and `:=` are one token unless an operator byte follows; then a run starts at the `:`.
fn colon<'input>(lex: &mut Lex<'input>) -> Token<'input> {
    let src = lex.source().as_bytes();
    let start = lex.span().start;
    match src.get(start + 1) {
        Some(b':' | b'=') if src.get(start + 2).copied().is_some_and(is_operator_byte) => {
            bump_operator_run(lex);
        }
        Some(b':' | b'=') => lex.bump(1),
        _ => {}
    }
    match lex.slice() {
        ":" => Token::Colon,
        "::" => Token::DoubleColon,
        run => Token::Operator(run),
    }
}

/// A number run is cut back to its last digit or `.`, except at end of input.
fn number<'input>(lex: &mut Lex<'input>) -> Token<'input> {
    let src = lex.source().as_bytes();
    let start = lex.span().start;
    let is_initial = |b: u8| b.is_ascii_digit() || b == b'.';
    let is_e = |b: u8| matches!(b, b'e' | b'E');
    let mut i = start + 1;
    let end = loop {
        let Some(&c) = src.get(i) else {
            break src.len();
        };
        let prev = src[i - 1];
        let continues = is_initial(c)
            || (c == b'_' && src.get(i + 1).copied().is_some_and(is_initial))
            || (is_e(c) && !is_e(prev) && (src[start].is_ascii_digit() || prev.is_ascii_digit()))
            || (matches!(c, b'+' | b'-') && is_e(prev));
        if !continues {
            while !is_initial(src[i - 1]) {
                i -= 1;
            }
            break i;
        }
        i += 1;
    };
    lex.bump(end - lex.span().end);
    match lex.slice() {
        "." => Token::Dot,
        text => Token::Number(text),
    }
}

/// `$` is a parameter marker unless `$tag$` opens a dollar-quoted string, which runs to the next
/// `$tag$`.
fn dollar<'input>(lex: &mut Lex<'input>) -> Result<Token<'input>, LexError> {
    let rest = lex.remainder();
    if rest.starts_with(|c: char| c.is_ascii_digit()) {
        return Ok(Token::Dollar);
    }
    let is_tag_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80;
    let tag_len = match rest.bytes().position(|b| !is_tag_byte(b)) {
        Some(n) if rest.as_bytes()[n] == b'$' => n,
        _ => return Ok(Token::Dollar),
    };
    let closing = format!("${}$", &rest[..tag_len]);
    let Some(pos) = rest[tag_len + 1..].find(&closing) else {
        lex.bump(rest.len());
        return Err(LexError::UnterminatedDollarString);
    };
    lex.bump(tag_len + 1 + pos + closing.len());
    Ok(Token::DollarString(lex.slice()))
}

/// Skips a nested `/* */` comment; the opening `/*` is already consumed.
fn block_comment(lex: &mut Lex<'_>) -> Result<Skip, LexError> {
    lex.extras.block_comment = Some(lex.span().start);
    let rest = lex.remainder().as_bytes();
    let mut depth = 1;
    let mut i = 0;
    while i + 1 < rest.len() {
        match &rest[i..i + 2] {
            b"/*" => {
                depth += 1;
                i += 2;
            }
            b"*/" => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    lex.bump(i);
                    return Ok(Skip);
                }
            }
            _ => i += 1,
        }
    }
    lex.bump(rest.len());
    Err(LexError::UnterminatedComment(lex.slice().to_owned()))
}

/// Rejects the zero-length `""` and ` `` `.
fn quoted_ident(raw: &str) -> Result<&str, LexError> {
    match raw {
        r#""""# | "``" => Err(LexError::ZeroLengthIdent),
        raw => Ok(raw),
    }
}

/// Adapts the Logos lexer to the `(start, token, end)` triples LALRPOP expects.
pub struct Lexer<'input> {
    tokens: SpannedIter<'input, Token<'input>>,
    /// End of the previous token when it was a single-quoted string, which a `'…'` may continue.
    string_end: Option<usize>,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            tokens: Token::lexer(input).spanned(),
            string_end: None,
        }
    }

    /// A `'…'` continues a single-quoted string before it when a newline and no block comment
    /// separate them.
    fn continues_string(&self, start: usize) -> bool {
        let Some(prev_end) = self.string_end else {
            return false;
        };
        let gap = &self.tokens.source().as_bytes()[prev_end..start];
        gap.iter().any(|b| matches!(b, b'\n' | b'\r'))
            && !self
                .tokens
                .extras
                .block_comment
                .is_some_and(|comment| (prev_end..start).contains(&comment))
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<(usize, Token<'input>, usize), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (token, span) = self.tokens.next()?;
        let token = match token {
            Ok(Token::String(raw)) if self.continues_string(span.start) => {
                Token::StringContinuation(raw)
            }
            Ok(token) => token,
            Err(err) => return Some(Err(ParseError::new(err.to_string(), span))),
        };
        let is_string = matches!(
            token,
            Token::String(_) | Token::StringContinuation(_) | Token::PrefixedString(_)
        );
        self.string_end = is_string.then_some(span.end);
        Some(Ok((span.start, token, span.end)))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use rstest::rstest;

    use super::*;

    fn spans(input: &str) -> Vec<(usize, &str)> {
        Lexer::new(input)
            .map(|item| {
                let (start, _, end) = item.unwrap();
                (start, &input[start..end])
            })
            .collect()
    }

    #[rstest]
    #[case("1 */ 2 <=> 3 || 4 ->> 5", &[(0, "1"), (2, "*/"), (5, "2"), (7, "<=>"), (11, "3"), (13, "||"), (16, "4"), (18, "->>"), (22, "5")])]
    #[case("1!=-1 2^-1 1@-2 2|-1", &[(0, "1"), (1, "!=-"), (4, "1"), (6, "2"), (7, "^-"), (9, "1"), (11, "1"), (12, "@-"), (14, "2"), (16, "2"), (17, "|-"), (19, "1")])]
    #[case("1<-2 1<=-2 1=-1 1*-2 1+-+-2", &[(0, "1"), (1, "<"), (2, "-"), (3, "2"), (5, "1"), (6, "<="), (8, "-"), (9, "2"), (11, "1"), (12, "="), (13, "-"), (14, "1"), (16, "1"), (17, "*"), (18, "-"), (19, "2"), (21, "1"), (22, "+"), (23, "-"), (24, "+"), (25, "-"), (26, "2")])]
    #[case("1+/*c*/2 1*/*c*/2 1+--c\n2", &[(0, "1"), (1, "+"), (7, "2"), (9, "1"), (10, "*"), (16, "2"), (18, "1"), (19, "+"), (24, "2")])]
    #[case("x=>-1 x:=-1 x:=~1 1 \\+ 2 \\*", &[(0, "x"), (1, "=>"), (3, "-"), (4, "1"), (6, "x"), (7, ":="), (9, "-"), (10, "1"), (12, "x"), (13, ":=~"), (16, "1"), (18, "1"), (20, "\\"), (21, "+"), (23, "2"), (25, "\\*")])]
    #[case("1::INT x:=1 [1,2,3][::-1] 1: :INT", &[(0, "1"), (1, "::"), (3, "INT"), (7, "x"), (8, ":="), (10, "1"), (12, "["), (13, "1"), (14, ","), (15, "2"), (16, ","), (17, "3"), (18, "]"), (19, "["), (20, ":"), (21, ":"), (22, "-"), (23, "1"), (24, "]"), (26, "1"), (27, ":"), (29, ":"), (30, "INT")])]
    #[case("a --c\r2 b", &[(0, "a"), (6, "2"), (8, "b")])]
    #[case("a\u{b}b\u{c}c", &[(0, "a"), (2, "b"), (4, "c")])]
    #[case("é1 中文 a$b _x ſelect", &[(0, "é1"), (4, "中文"), (11, "a$b"), (15, "_x"), (18, "ſelect")])]
    #[case("a\u{1}b \u{7f}c", &[(0, "a\u{1}b"), (4, "\u{7f}c")])]
    #[case("\"a\"\"b\" `c``d` \"e`f\"", &[(0, "\"a\"\"b\""), (7, "`c``d`"), (14, "\"e`f\"")])]
    #[case("E'a\\'b' x'4142' n'z' B'101' Ex'1' U&'x'", &[(0, "E'a\\'b'"), (8, "x'4142'"), (16, "n'z'"), (21, "B'101'"), (28, "Ex"), (30, "'1'"), (34, "U"), (35, "&"), (36, "'x'")])]
    #[case("$$x$$ $t$y$t$ $Tag$x$tag$ $Tag$ $1 $name", &[(0, "$$x$$"), (6, "$t$y$t$"), (14, "$Tag$x$tag$ $Tag$"), (32, "$"), (33, "1"), (35, "$"), (36, "name")])]
    #[case("1.2.3 ._0 1e5e3 1_000_000 1__0 .e5 1_ 1.5_", &[(0, "1.2.3"), (6, "._0"), (10, "1e5e3"), (16, "1_000_000"), (26, "1"), (27, "__0"), (31, "."), (32, "e5"), (35, "1"), (36, "_"), (38, "1.5"), (41, "_")])]
    #[case("1e5 1.e5 1e; 1ee5 1e1_0 0x1F 1a 1.a a..b", &[(0, "1e5"), (4, "1.e5"), (9, "1"), (10, "e"), (11, ";"), (13, "1"), (14, "ee5"), (18, "1e1_0"), (24, "0"), (25, "x1F"), (29, "1"), (30, "a"), (32, "1."), (34, "a"), (36, "a"), (37, ".."), (39, "b")])]
    #[case("1e+;", &[(0, "1"), (1, "e"), (2, "+"), (3, ";")])]
    #[case("1e", &[(0, "1e")])]
    #[case("1e-", &[(0, "1e-")])]
    #[case("t._0a t.a1 t.e1 t. a1", &[(0, "t"), (1, "._0"), (4, "a"), (6, "t"), (7, "."), (8, "a1"), (11, "t"), (12, "."), (13, "e1"), (16, "t"), (17, "."), (19, "a1")])]
    #[case("? ?1 #1 # 2", &[(0, "?"), (2, "?"), (3, "1"), (5, "#"), (6, "1"), (8, "#"), (10, "2")])]
    #[case("'a'\n'b' 'c' -- x /* y\n'd' /* z */\n'e'", &[(0, "'a'"), (4, "'b'"), (8, "'c'"), (22, "'d'"), (34, "'e'")])]
    #[case("select 1; select 2;;", &[(0, "select"), (7, "1"), (8, ";"), (10, "select"), (17, "2"), (18, ";"), (19, ";")])]
    #[case("SELECT DATE'2024-01-01'", &[(0, "SELECT"), (7, "DATE"), (11, "'2024-01-01'")])]
    #[case("1 /* a /* b */ c */ 2", &[(0, "1"), (20, "2")])]
    #[case("'it''s' 'a\nb' ''", &[(0, "'it''s'"), (8, "'a\nb'"), (14, "''")])]
    #[case("a == b <> c != d // e % f", &[(0, "a"), (2, "=="), (5, "b"), (7, "<>"), (10, "c"), (12, "!="), (15, "d"), (17, "//"), (20, "e"), (22, "%"), (24, "f")])]
    fn test_lex_spans(#[case] input: &str, #[case] expected: &[(usize, &str)]) {
        assert_eq!(spans(input), expected);
    }

    #[rstest]
    #[case(
        "SeLeCt select$ \"select\"",
        vec![Token::Select, Token::Ident("select$"), Token::QuotedIdent("\"select\"")],
    )]
    #[case(
        "E'a\\'b' X'41' $t$y$t$",
        vec![Token::PrefixedString("E'a\\'b'"), Token::PrefixedString("X'41'"), Token::DollarString("$t$y$t$")],
    )]
    #[case(
        "'a'\n'b' 'c' -- x /* y\n'd' /* z */\n'e'",
        vec![
            Token::String("'a'"),
            Token::StringContinuation("'b'"),
            Token::String("'c'"),
            Token::StringContinuation("'d'"),
            Token::String("'e'"),
        ],
    )]
    #[case("1. . .. ::", vec![Token::Number("1."), Token::Dot, Token::Number(".."), Token::DoubleColon])]
    #[case(
        "x:=1 a<=b",
        vec![
            Token::Ident("x"),
            Token::Operator(":="),
            Token::Number("1"),
            Token::Ident("a"),
            Token::Operator("<="),
            Token::Ident("b"),
        ],
    )]
    #[case("$1 $", vec![Token::Dollar, Token::Number("1"), Token::Dollar])]
    #[case("x =\n'b'", vec![Token::Ident("x"), Token::Operator("="), Token::String("'b'")])]
    fn test_lex_kinds(#[case] input: &str, #[case] expected: Vec<Token<'_>>) {
        let tokens: Vec<_> = Lexer::new(input).map(|item| item.unwrap().1).collect();
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_unicode_space_is_whitespace() {
        assert_eq!(spans("a\u{A0}b\u{3000}c"), [(0, "a"), (3, "b"), (7, "c")]);
    }

    #[test]
    fn test_every_ascii_byte_lexes() {
        for byte in 0..=0x7F_u8 {
            let input = char::from(byte).to_string();
            let err = Lexer::new(&input).find_map(Result::err);
            assert!(
                err.is_none_or(|err| err.message != LexError::UnexpectedChar.to_string()),
                "byte {byte:#04x}"
            );
        }
    }

    #[rstest]
    #[case("/* open", "unterminated /* comment at or near \"/* open\"", 0..7)]
    #[case("/* a /* b */", "unterminated /* comment at or near \"/* a /* b */\"", 0..12)]
    #[case("'open", "unterminated string literal", 0..5)]
    #[case("'a''", "unterminated string literal", 0..4)]
    #[case("E'a\\'", "unterminated string literal", 0..5)]
    #[case("x'4", "unterminated string literal", 0..3)]
    #[case("\"open", "unterminated quoted identifier", 0..5)]
    #[case("`open", "unterminated quoted identifier", 0..5)]
    #[case("\"\"", "zero-length delimited identifier", 0..2)]
    #[case("``", "zero-length delimited identifier", 0..2)]
    #[case("$$abc", "unterminated dollar-quoted string", 0..5)]
    #[case("$a$ $b$", "unterminated dollar-quoted string", 0..7)]
    fn test_lex_error(#[case] input: &str, #[case] message: &str, #[case] span: Range<usize>) {
        let err = Lexer::new(input).find_map(Result::err).unwrap();
        assert_eq!(err, ParseError::new(message, span));
    }
}
