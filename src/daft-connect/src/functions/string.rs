use daft_functions_utf8::*;

use super::FunctionModule;
// see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions
pub struct StringFunctions;

impl FunctionModule for StringFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_todo_fn("ascii");
        parent.add_todo_fn("base64");
        parent.add_todo_fn("bit_length");
        parent.add_todo_fn("btrim");
        parent.add_todo_fn("char");
        parent.add_fn("character_length", Length);
        parent.add_fn("char_length", Length);
        parent.add_todo_fn("concat_ws");
        parent.add_fn("contains", Contains);
        parent.add_todo_fn("decode");
        parent.add_todo_fn("elt");
        parent.add_todo_fn("encode");
        parent.add_fn("endswith", EndsWith);
        parent.add_todo_fn("find_in_set");
        parent.add_todo_fn("format_number");
        parent.add_todo_fn("format_string");
        parent.add_fn("ilike", ILike);
        parent.add_todo_fn("initcap");
        parent.add_todo_fn("instr");
        parent.add_todo_fn("lcase");
        parent.add_fn("length", LengthBytes);
        parent.add_fn("like", Like);
        parent.add_fn("lower", Lower);
        parent.add_fn("left", Left);
        parent.add_todo_fn("levenshtein");
        parent.add_todo_fn("locate");
        parent.add_fn("lpad", LPad);
        parent.add_todo_fn("ltrim");
        parent.add_todo_fn("mask");
        parent.add_todo_fn("octet_length");
        parent.add_todo_fn("parse_url");
        parent.add_todo_fn("position");
        parent.add_todo_fn("printf");
        parent.add_todo_fn("rlike");
        parent.add_todo_fn("regexp");
        parent.add_todo_fn("regexp_like");
        parent.add_todo_fn("regexp_count");
        parent.add_fn("regexp_extract", RegexpExtract);
        parent.add_fn("regexp_extract_all", RegexpExtractAll);
        parent.add_fn("regexp_replace", RegexpReplace);
        parent.add_todo_fn("regexp_substr");
        parent.add_todo_fn("regexp_instr");
        parent.add_fn("replace", Replace);
        parent.add_fn("right", Right);
        parent.add_todo_fn("ucase");
        parent.add_todo_fn("unbase64");
        parent.add_fn("rpad", RPad);
        parent.add_todo_fn("repeat");
        parent.add_todo_fn("rtrim");
        parent.add_todo_fn("soundex");
        parent.add_fn("split", Split);
        parent.add_todo_fn("split_part");
        parent.add_fn("startswith", StartsWith);
        parent.add_fn("substr", Substr);
        parent.add_fn("substring", Substr);
        parent.add_todo_fn("substring_index");
        parent.add_todo_fn("overlay");
        parent.add_todo_fn("sentences");
        parent.add_todo_fn("to_binary");
        parent.add_todo_fn("to_char");
        parent.add_todo_fn("to_number");
        parent.add_todo_fn("to_varchar");
        parent.add_todo_fn("translate");
        parent.add_todo_fn("trim");
        parent.add_fn("upper", Upper);
        parent.add_todo_fn("url_decode");
        parent.add_todo_fn("url_encode");
    }
}
