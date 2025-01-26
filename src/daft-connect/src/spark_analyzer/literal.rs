use daft_core::datatypes::IntervalValue;
use spark_connect::expression::{literal::LiteralType, Literal};

use crate::{error::ConnectResult, invalid_relation_err, not_yet_implemented};

// todo(test): add tests for this esp in Python
pub fn to_daft_literal(literal: &Literal) -> ConnectResult<daft_dsl::ExprRef> {
    let Some(literal) = &literal.literal_type else {
        invalid_relation_err!("Literal is required");
    };

    match literal {
        LiteralType::Array(_) => not_yet_implemented!("array literals"),
        LiteralType::Binary(bytes) => Ok(daft_dsl::lit(bytes.as_slice())),
        LiteralType::Boolean(b) => Ok(daft_dsl::lit(*b)),
        LiteralType::Byte(_) => not_yet_implemented!("Byte literals"),
        LiteralType::CalendarInterval(_) => not_yet_implemented!("Calendar interval literals"),
        LiteralType::Date(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::DayTimeInterval(_) => not_yet_implemented!("Day-time interval literals"),
        LiteralType::Decimal(_) => not_yet_implemented!("Decimal literals"),
        LiteralType::Double(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::Float(f) => {
            let f = f64::from(*f);
            Ok(daft_dsl::lit(f))
        }
        LiteralType::Integer(i) => Ok(daft_dsl::lit(*i)),
        LiteralType::Long(l) => Ok(daft_dsl::lit(*l)),
        LiteralType::Map(_) => not_yet_implemented!("Map literals"),
        LiteralType::Null(_) => {
            // todo(correctness): is it ok to assume type is i32 here?
            Ok(daft_dsl::null_lit())
        }
        LiteralType::Short(_) => not_yet_implemented!("Short literals"),
        LiteralType::String(s) => Ok(daft_dsl::lit(s.as_str())),
        LiteralType::Struct(_) => not_yet_implemented!("Struct literals"),
        LiteralType::Timestamp(ts) => {
            // todo(correctness): is it ok that the type is different logically?
            Ok(daft_dsl::lit(*ts))
        }
        LiteralType::TimestampNtz(ts) => {
            // todo(correctness): is it ok that the type is different logically?
            Ok(daft_dsl::lit(*ts))
        }
        LiteralType::YearMonthInterval(value) => {
            let interval = IntervalValue::new(*value, 0, 0);
            Ok(daft_dsl::lit(interval))
        }
    }
}
