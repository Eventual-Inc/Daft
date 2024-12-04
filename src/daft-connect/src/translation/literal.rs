use daft_core::datatypes::IntervalValue;
use eyre::bail;
use spark_connect::expression::{literal::LiteralType, Literal};

// todo(test): add tests for this esp in Python
pub fn to_daft_literal(literal: &Literal) -> eyre::Result<daft_dsl::ExprRef> {
    let Some(literal) = &literal.literal_type else {
        bail!("Literal is required");
    };

    match literal {
        LiteralType::Array(_) => bail!("Array literals not yet supported"),
        LiteralType::Binary(bytes) => Ok(daft_dsl::lit(bytes.as_slice())),
        LiteralType::Boolean(b) => Ok(daft_dsl::lit(*b)),
        LiteralType::Byte(_) => bail!("Byte literals not yet supported"),
        LiteralType::CalendarInterval(_) => {
            bail!("Calendar interval literals not yet supported")
        }
        LiteralType::Date(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::DayTimeInterval(_) => {
            bail!("Day-time interval literals not yet supported")
        }
        LiteralType::Decimal(_) => bail!("Decimal literals not yet supported"),
        LiteralType::Double(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::Float(f) => {
            let f = f64::from(*f);
            Ok(daft_dsl::lit(f))
        }
        LiteralType::Integer(i) => Ok(daft_dsl::lit(*i)),
        LiteralType::Long(l) => Ok(daft_dsl::lit(*l)),
        LiteralType::Map(_) => bail!("Map literals not yet supported"),
        LiteralType::Null(_) => {
            // todo(correctness): is it ok to assume type is i32 here?
            Ok(daft_dsl::null_lit())
        }
        LiteralType::Short(_) => bail!("Short literals not yet supported"),
        LiteralType::String(s) => Ok(daft_dsl::lit(s.as_str())),
        LiteralType::Struct(_) => bail!("Struct literals not yet supported"),
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
