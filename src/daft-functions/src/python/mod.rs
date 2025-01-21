macro_rules! simple_python_wrapper {
    (
        $fn_name:ident
        , $function:path
        , [$($arg:ident: $t:ty),* $(,)?]
        $(,)?
    ) => {
        #[pyfunction]
        pub fn $fn_name($($arg: $t),*) -> PyResult<PyExpr> {
            Ok($function($($arg.into()),*).into())
        }
    };
}

mod binary;
mod coalesce;
mod distance;
mod float;
mod image;
mod list;
mod misc;
mod numeric;
mod temporal;
mod tokenize;
mod uri;
mod utf8;

use pyo3::{
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};

pub fn register(parent: &Bound<PyModule>) -> PyResult<()> {
    macro_rules! add {
        ($p:path) => {
            parent.add_function(wrap_pyfunction!($p, parent)?)?;
        };
    }

    add!(coalesce::coalesce);
    add!(distance::cosine_distance);
    add!(binary::binary_length);
    add!(binary::binary_concat);
    add!(binary::binary_slice);

    add!(float::is_inf);
    add!(float::is_nan);
    add!(float::not_nan);
    add!(float::fill_nan);

    add!(image::image_crop);
    add!(image::image_to_mode);
    add!(image::image_decode);
    add!(image::image_encode);
    add!(image::image_resize);

    add!(list::list_chunk);
    add!(list::list_count);
    add!(list::explode);
    add!(list::list_get);
    add!(list::list_join);
    add!(list::list_max);
    add!(list::list_mean);
    add!(list::list_min);
    add!(list::list_slice);
    add!(list::list_sort);
    add!(list::list_sum);
    add!(list::list_unique_count);
    add!(list::list_value_counts);

    add!(misc::to_struct);
    add!(misc::utf8_count_matches);
    add!(misc::hash);
    add!(misc::minhash);

    add!(numeric::abs);
    add!(numeric::cbrt);
    add!(numeric::ceil);
    add!(numeric::clip);
    add!(numeric::exp);
    add!(numeric::floor);
    add!(numeric::sign);
    add!(numeric::sqrt);
    add!(numeric::log2);
    add!(numeric::log10);
    add!(numeric::log);
    add!(numeric::ln);
    add!(numeric::sin);
    add!(numeric::cos);
    add!(numeric::tan);
    add!(numeric::cot);
    add!(numeric::arcsin);
    add!(numeric::arccos);
    add!(numeric::arctan);
    add!(numeric::radians);
    add!(numeric::degrees);
    add!(numeric::arcsinh);
    add!(numeric::arccosh);
    add!(numeric::arctanh);
    add!(numeric::arctan2);
    add!(numeric::round);

    add!(temporal::dt_date);
    add!(temporal::dt_day);
    add!(temporal::dt_day_of_week);
    add!(temporal::dt_hour);
    add!(temporal::dt_minute);
    add!(temporal::dt_month);
    add!(temporal::dt_second);
    add!(temporal::dt_time);
    add!(temporal::dt_year);
    add!(temporal::dt_truncate);

    add!(tokenize::tokenize_encode);
    add!(tokenize::tokenize_decode);

    add!(uri::url_download);
    add!(uri::url_upload);

    add!(utf8::utf8_capitalize);
    add!(utf8::utf8_contains);
    add!(utf8::utf8_endswith);
    add!(utf8::utf8_extract);
    add!(utf8::utf8_extract_all);
    add!(utf8::utf8_find);
    add!(utf8::utf8_ilike);
    add!(utf8::utf8_left);
    add!(utf8::utf8_length);
    add!(utf8::utf8_length_bytes);
    add!(utf8::utf8_like);
    add!(utf8::utf8_lower);
    add!(utf8::utf8_lpad);
    add!(utf8::utf8_lstrip);
    add!(utf8::utf8_match);
    add!(utf8::utf8_repeat);
    add!(utf8::utf8_replace);
    add!(utf8::utf8_reverse);
    add!(utf8::utf8_right);
    add!(utf8::utf8_rpad);
    add!(utf8::utf8_rstrip);
    add!(utf8::utf8_split);
    add!(utf8::utf8_startswith);
    add!(utf8::utf8_substr);
    add!(utf8::utf8_upper);
    add!(utf8::utf8_normalize);
    add!(utf8::utf8_to_date);
    add!(utf8::utf8_to_datetime);

    Ok(())
}
