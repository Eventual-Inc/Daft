use daft_dsl::LiteralValue;
use daft_functions::utf8::{
    extract, extract_all, Utf8Endswith, Utf8Ilike, Utf8Left, Utf8Length, Utf8LengthBytes, Utf8Like,
    Utf8Lower, Utf8Lpad, Utf8Replace, Utf8Right, Utf8Rpad, Utf8Split, Utf8Startswith, Utf8Substr,
    Utf8Upper,
};
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction, TODO_FUNCTION};
use crate::{
    error::ConnectResult, invalid_argument_err, spark_analyzer::expr_analyzer::analyze_expr,
};

// see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions
pub struct StringFunctions;

impl FunctionModule for StringFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("ascii", TODO_FUNCTION);
        parent.add_fn("base64", TODO_FUNCTION);
        parent.add_fn("bit_length", TODO_FUNCTION);
        parent.add_fn("btrim", TODO_FUNCTION);
        parent.add_fn("char", TODO_FUNCTION);
        parent.add_fn("character_length", Utf8Length {});
        parent.add_fn("char_length", Utf8Length {});
        parent.add_fn("concat_ws", TODO_FUNCTION);
        parent.add_fn("contains", daft_functions::utf8::Utf8Contains {});
        parent.add_fn("decode", TODO_FUNCTION);
        parent.add_fn("elt", TODO_FUNCTION);
        parent.add_fn("encode", Utf8Endswith {});
        parent.add_fn("endswith", TODO_FUNCTION);
        parent.add_fn("find_in_set", TODO_FUNCTION);
        parent.add_fn("format_number", TODO_FUNCTION);
        parent.add_fn("format_string", TODO_FUNCTION);
        parent.add_fn("ilike", Utf8Ilike {});
        parent.add_fn("initcap", TODO_FUNCTION);
        parent.add_fn("instr", TODO_FUNCTION);
        parent.add_fn("lcase", TODO_FUNCTION);
        parent.add_fn("length", Utf8LengthBytes {});
        parent.add_fn("like", Utf8Like {});
        parent.add_fn("lower", Utf8Lower {});
        parent.add_fn("left", Utf8Left {});
        parent.add_fn("levenshtein", TODO_FUNCTION);
        parent.add_fn("locate", TODO_FUNCTION);
        parent.add_fn("lpad", Utf8Lpad {});
        parent.add_fn("ltrim", TODO_FUNCTION);
        parent.add_fn("mask", TODO_FUNCTION);
        parent.add_fn("octet_length", TODO_FUNCTION);
        parent.add_fn("parse_url", TODO_FUNCTION);
        parent.add_fn("position", TODO_FUNCTION);
        parent.add_fn("printf", TODO_FUNCTION);
        parent.add_fn("rlike", TODO_FUNCTION);
        parent.add_fn("regexp", TODO_FUNCTION);
        parent.add_fn("regexp_like", TODO_FUNCTION);
        parent.add_fn("regexp_count", TODO_FUNCTION);
        parent.add_fn("regexp_extract", RegexpExtract);
        parent.add_fn("regexp_extract_all", RegexpExtractAll);
        parent.add_fn("regexp_replace", Utf8Replace { regex: true });
        parent.add_fn("regexp_substr", TODO_FUNCTION);
        parent.add_fn("regexp_instr", TODO_FUNCTION);
        parent.add_fn("replace", Utf8Replace { regex: false });
        parent.add_fn("right", Utf8Right {});
        parent.add_fn("ucase", TODO_FUNCTION);
        parent.add_fn("unbase64", TODO_FUNCTION);
        parent.add_fn("rpad", Utf8Rpad {});
        parent.add_fn("repeat", TODO_FUNCTION);
        parent.add_fn("rtrim", TODO_FUNCTION);
        parent.add_fn("soundex", TODO_FUNCTION);
        parent.add_fn("split", Utf8Split { regex: false });
        parent.add_fn("split_part", TODO_FUNCTION);
        parent.add_fn("startswith", Utf8Startswith {});
        parent.add_fn("substr", Utf8Substr {});
        parent.add_fn("substring", Utf8Substr {});
        parent.add_fn("substring_index", TODO_FUNCTION);
        parent.add_fn("overlay", TODO_FUNCTION);
        parent.add_fn("sentences", TODO_FUNCTION);
        parent.add_fn("to_binary", TODO_FUNCTION);
        parent.add_fn("to_char", TODO_FUNCTION);
        parent.add_fn("to_number", TODO_FUNCTION);
        parent.add_fn("to_varchar", TODO_FUNCTION);
        parent.add_fn("translate", TODO_FUNCTION);
        parent.add_fn("trim", TODO_FUNCTION);
        parent.add_fn("upper", Utf8Upper {});
        parent.add_fn("url_decode", TODO_FUNCTION);
        parent.add_fn("url_encode", TODO_FUNCTION);
    }
}

struct RegexpExtract;
impl SparkFunction for RegexpExtract {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
            .collect::<ConnectResult<Vec<_>>>()?;

        let [input, pattern, idx] = args.as_slice() else {
            invalid_argument_err!("regexp_extract requires exactly 3 arguments");
        };

        let idx = match idx.as_ref().as_literal() {
            Some(LiteralValue::Int8(i)) => *i as usize,
            Some(LiteralValue::UInt8(u)) => *u as usize,
            Some(LiteralValue::Int16(i)) => *i as usize,
            Some(LiteralValue::UInt16(u)) => *u as usize,
            Some(LiteralValue::Int32(i)) => *i as usize,
            Some(LiteralValue::UInt32(u)) => *u as usize,
            Some(LiteralValue::Int64(i)) => *i as usize,
            Some(LiteralValue::UInt64(u)) => *u as usize,
            _ => invalid_argument_err!("regexp_extract index must be a number"),
        };
        Ok(extract(input.clone(), pattern.clone(), idx))
    }
}

struct RegexpExtractAll;
impl SparkFunction for RegexpExtractAll {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
            .collect::<ConnectResult<Vec<_>>>()?;

        let [input, pattern, idx] = args.as_slice() else {
            invalid_argument_err!("regexp_extract requires exactly 3 arguments");
        };

        let idx = match idx.as_ref().as_literal() {
            Some(LiteralValue::Int8(i)) => *i as usize,
            Some(LiteralValue::UInt8(u)) => *u as usize,
            Some(LiteralValue::Int16(i)) => *i as usize,
            Some(LiteralValue::UInt16(u)) => *u as usize,
            Some(LiteralValue::Int32(i)) => *i as usize,
            Some(LiteralValue::UInt32(u)) => *u as usize,
            Some(LiteralValue::Int64(i)) => *i as usize,
            Some(LiteralValue::UInt64(u)) => *u as usize,
            _ => invalid_argument_err!("regexp_extract index must be a number"),
        };
        Ok(extract_all(input.clone(), pattern.clone(), idx))
    }
}
