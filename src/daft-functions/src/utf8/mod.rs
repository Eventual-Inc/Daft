mod capitalize;
mod contains;
mod endswith;
mod extract;
mod extract_all;
mod find;
mod ilike;
mod left;
mod length;
mod length_bytes;
mod like;
mod lower;
mod lpad;
mod lstrip;
mod match_;
mod normalize;
mod repeat;
mod replace;
mod reverse;
mod right;
mod rpad;
mod rstrip;
mod split;
mod startswith;
mod substr;
mod to_date;
mod to_datetime;
mod upper;

pub use capitalize::{utf8_capitalize as capitalize, Utf8Capitalize};
pub use contains::{utf8_contains as contains, Utf8Contains};
pub use endswith::{utf8_endswith as endswith, Utf8Endswith};
pub use extract::{utf8_extract as extract, Utf8Extract};
pub use extract_all::{utf8_extract_all as extract_all, Utf8ExtractAll};
pub use find::{utf8_find as find, Utf8Find};
pub use ilike::{utf8_ilike as ilike, Utf8Ilike};
pub use left::{utf8_left as left, Utf8Left};
pub use length::{utf8_length as length, Utf8Length};
pub use length_bytes::{utf8_length_bytes as length_bytes, Utf8LengthBytes};
pub use like::{utf8_like as like, Utf8Like};
pub use lower::{utf8_lower as lower, Utf8Lower};
pub use lpad::{utf8_lpad as lpad, Utf8Lpad};
pub use lstrip::{utf8_lstrip as lstrip, Utf8Lstrip};
pub use match_::{utf8_match as match_, Utf8Match};
pub use normalize::{utf8_normalize as normalize, Utf8Normalize};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use repeat::{utf8_repeat as repeat, Utf8Repeat};
pub use replace::{utf8_replace as replace, Utf8Replace};
pub use reverse::{utf8_reverse as reverse, Utf8Reverse};
pub use right::{utf8_right as right, Utf8Right};
pub use rpad::{utf8_rpad as rpad, Utf8Rpad};
pub use rstrip::{utf8_rstrip as rstrip, Utf8Rstrip};
pub use split::{utf8_split as split, Utf8Split};
pub use startswith::{utf8_startswith as startswith, Utf8Startswith};
pub use substr::{utf8_substr as substr, Utf8Substr};
pub use to_date::{utf8_to_date as to_date, Utf8ToDate};
pub use to_datetime::{utf8_to_datetime as to_datetime, Utf8ToDatetime};
pub use upper::{utf8_upper as upper, Utf8Upper};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(
        capitalize::py_utf8_capitalize,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(contains::py_utf8_contains, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(endswith::py_utf8_endswith, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(extract::py_utf8_extract, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        extract_all::py_utf8_extract_all,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(find::py_utf8_find, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(like::py_utf8_like, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(ilike::py_utf8_ilike, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(left::py_utf8_left, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(length::py_utf8_length, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        length_bytes::py_utf8_length_bytes,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(lower::py_utf8_lower, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(lpad::py_utf8_lpad, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(lstrip::py_utf8_lstrip, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(match_::py_utf8_match, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        normalize::py_utf8_normalize,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(repeat::py_utf8_repeat, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(replace::py_utf8_replace, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(reverse::py_utf8_reverse, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(right::py_utf8_right, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(rpad::py_utf8_rpad, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(rstrip::py_utf8_rstrip, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(split::py_utf8_split, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        startswith::py_utf8_startswith,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(substr::py_utf8_substr, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(to_date::py_utf8_to_date, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        to_datetime::py_utf8_to_datetime,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(upper::py_utf8_upper, parent)?)?;

    Ok(())
}
