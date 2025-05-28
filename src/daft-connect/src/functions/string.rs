use daft_functions_utf8::*;

use super::{FunctionModule, TODO_FUNCTION};
// see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions
pub struct StringFunctions;

impl FunctionModule for StringFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("ascii", TODO_FUNCTION);
        parent.add_fn("base64", TODO_FUNCTION);
        parent.add_fn("bit_length", TODO_FUNCTION);
        parent.add_fn("btrim", TODO_FUNCTION);
        parent.add_fn("char", TODO_FUNCTION);
        parent.add_fn("character_length", Length);
        parent.add_fn("char_length", Length);
        parent.add_fn("concat_ws", TODO_FUNCTION);
        parent.add_fn("contains", Contains);
        parent.add_fn("decode", TODO_FUNCTION);
        parent.add_fn("elt", TODO_FUNCTION);
        parent.add_fn("encode", TODO_FUNCTION);
        parent.add_fn("endswith", EndsWith);
        parent.add_fn("find_in_set", TODO_FUNCTION);
        parent.add_fn("format_number", TODO_FUNCTION);
        parent.add_fn("format_string", TODO_FUNCTION);
        parent.add_fn("ilike", ILike);
        parent.add_fn("initcap", TODO_FUNCTION);
        parent.add_fn("instr", TODO_FUNCTION);
        parent.add_fn("lcase", TODO_FUNCTION);
        parent.add_fn("length", LengthBytes);
        parent.add_fn("like", Like);
        parent.add_fn("lower", Lower);
        parent.add_fn("left", Left);
        parent.add_fn("levenshtein", TODO_FUNCTION);
        parent.add_fn("locate", TODO_FUNCTION);
        parent.add_fn("lpad", LPad);
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
        parent.add_fn("regexp_replace", RegexpReplace);
        parent.add_fn("regexp_substr", TODO_FUNCTION);
        parent.add_fn("regexp_instr", TODO_FUNCTION);
        parent.add_fn("replace", Replace);
        parent.add_fn("right", Right);
        parent.add_fn("ucase", TODO_FUNCTION);
        parent.add_fn("unbase64", TODO_FUNCTION);
        parent.add_fn("rpad", RPad);
        parent.add_fn("repeat", TODO_FUNCTION);
        parent.add_fn("rtrim", TODO_FUNCTION);
        parent.add_fn("soundex", TODO_FUNCTION);
        parent.add_fn("split", Split);
        parent.add_fn("split_part", TODO_FUNCTION);
        parent.add_fn("startswith", StartsWith);
        parent.add_fn("substr", Substr);
        parent.add_fn("substring", Substr);
        parent.add_fn("substring_index", TODO_FUNCTION);
        parent.add_fn("overlay", TODO_FUNCTION);
        parent.add_fn("sentences", TODO_FUNCTION);
        parent.add_fn("to_binary", TODO_FUNCTION);
        parent.add_fn("to_char", TODO_FUNCTION);
        parent.add_fn("to_number", TODO_FUNCTION);
        parent.add_fn("to_varchar", TODO_FUNCTION);
        parent.add_fn("translate", TODO_FUNCTION);
        parent.add_fn("trim", TODO_FUNCTION);
        parent.add_fn("upper", Upper);
        parent.add_fn("url_decode", TODO_FUNCTION);
        parent.add_fn("url_encode", TODO_FUNCTION);
    }
}
