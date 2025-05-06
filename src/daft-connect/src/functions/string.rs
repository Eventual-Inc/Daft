use daft_functions::utf8::{
    Utf8Lpad, Utf8Replace, Utf8Right, Utf8Rpad, Utf8Split, Utf8Substr, Utf8Upper,
};

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
        parent.add_fn("character_length", daft_functions_utf8::Length);
        parent.add_fn("char_length", daft_functions_utf8::Length);
        parent.add_fn("concat_ws", TODO_FUNCTION);
        parent.add_fn("contains", daft_functions_utf8::Contains);
        parent.add_fn("decode", TODO_FUNCTION);
        parent.add_fn("elt", TODO_FUNCTION);
        parent.add_fn("encode", TODO_FUNCTION);
        parent.add_fn("endswith", daft_functions_utf8::EndsWith);
        parent.add_fn("find_in_set", TODO_FUNCTION);
        parent.add_fn("format_number", TODO_FUNCTION);
        parent.add_fn("format_string", TODO_FUNCTION);
        parent.add_fn("ilike", daft_functions_utf8::ILike);
        parent.add_fn("initcap", TODO_FUNCTION);
        parent.add_fn("instr", TODO_FUNCTION);
        parent.add_fn("lcase", TODO_FUNCTION);
        parent.add_fn("length", daft_functions_utf8::LengthBytes);
        parent.add_fn("like", daft_functions_utf8::Like);
        parent.add_fn("lower", daft_functions_utf8::Lower);
        parent.add_fn("left", daft_functions_utf8::Left);
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
        parent.add_fn("regexp_extract", daft_functions_utf8::RegexpExtract);
        parent.add_fn("regexp_extract_all", daft_functions_utf8::RegexpExtractAll);
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
        parent.add_fn("startswith", daft_functions_utf8::StartsWith);
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
