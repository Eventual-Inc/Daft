use daft_dsl::LiteralValue;
use daft_functions::numeric::{
    abs::Abs,
    cbrt::Cbrt,
    ceil::Ceil,
    exp::Exp,
    floor::Floor,
    log::{log, Ln, Log10, Log2},
    round::round,
    sqrt::Sqrt,
    trigonometry::{
        ArcCos, ArcCosh, ArcSin, ArcSinh, ArcTan, ArcTanh, Atan2, Cos, Cot, Degrees, Radians, Sin,
        Tan,
    },
};
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction, TODO_FUNCTION};
use crate::{
    error::{ConnectError, ConnectResult},
    invalid_argument_err,
    spark_analyzer::SparkAnalyzer,
};

// see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#math-functions
pub struct MathFunctions;

impl FunctionModule for MathFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("sqrt", Sqrt {});
        parent.add_fn("abs", Abs {});
        parent.add_fn("acos", ArcCos);
        parent.add_fn("acosh", ArcCosh);
        parent.add_fn("asin", ArcSin);
        parent.add_fn("asinh", ArcSinh);
        parent.add_fn("atan", ArcTan);
        parent.add_fn("atanh", ArcTanh);
        parent.add_fn("atan2", Atan2 {});
        parent.add_fn("bin", TODO_FUNCTION);
        parent.add_fn("cbrt", Cbrt {});
        parent.add_fn("ceil", Ceil {});
        parent.add_fn("ceiling", Ceil {});
        parent.add_fn("conv", TODO_FUNCTION);
        parent.add_fn("cos", Cos {});
        parent.add_fn("cosh", TODO_FUNCTION);
        parent.add_fn("cot", Cot {});
        parent.add_fn("csc", TODO_FUNCTION);
        parent.add_fn("e", TODO_FUNCTION);
        parent.add_fn("exp", Exp {});
        parent.add_fn("expm1", TODO_FUNCTION);
        parent.add_fn("factorial", TODO_FUNCTION);
        parent.add_fn("floor", Floor {});
        parent.add_fn("hex", TODO_FUNCTION);
        parent.add_fn("unhex", TODO_FUNCTION);
        parent.add_fn("hypot", TODO_FUNCTION);
        parent.add_fn("ln", Ln {});
        parent.add_fn("log", LogFunction);
        parent.add_fn("log10", Log10 {});
        parent.add_fn("log1p", TODO_FUNCTION);
        parent.add_fn("log2", Log2 {});
        parent.add_fn("negate", TODO_FUNCTION);
        parent.add_fn("negative", TODO_FUNCTION);
        parent.add_fn("pi", TODO_FUNCTION);
        parent.add_fn("pmod", TODO_FUNCTION);
        parent.add_fn("positive", TODO_FUNCTION);
        parent.add_fn("pow", TODO_FUNCTION);
        parent.add_fn("power", TODO_FUNCTION);
        parent.add_fn("rint", TODO_FUNCTION);
        parent.add_fn("round", RoundFunction);
        parent.add_fn("bround", TODO_FUNCTION);
        parent.add_fn("sec", TODO_FUNCTION);
        parent.add_fn("shiftleft", TODO_FUNCTION);
        parent.add_fn("shiftright", TODO_FUNCTION);
        parent.add_fn("sign", TODO_FUNCTION);
        parent.add_fn("signum", TODO_FUNCTION);
        parent.add_fn("sin", Sin {});
        parent.add_fn("sinh", TODO_FUNCTION);
        parent.add_fn("tan", Tan {});
        parent.add_fn("tanh", TODO_FUNCTION);
        parent.add_fn("toDegrees", TODO_FUNCTION);
        parent.add_fn("try_add", TODO_FUNCTION);
        parent.add_fn("try_avg", TODO_FUNCTION);
        parent.add_fn("try_divide", TODO_FUNCTION);
        parent.add_fn("try_multiply", TODO_FUNCTION);
        parent.add_fn("try_subtract", TODO_FUNCTION);
        parent.add_fn("try_sum", TODO_FUNCTION);
        parent.add_fn("try_to_binary", TODO_FUNCTION);
        parent.add_fn("try_to_number", TODO_FUNCTION);
        parent.add_fn("degrees", Degrees {});
        parent.add_fn("toRadians", TODO_FUNCTION);
        parent.add_fn("radians", Radians {});
        parent.add_fn("width_bucket", TODO_FUNCTION);
        //
    }
}

struct LogFunction;
impl SparkFunction for LogFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(|arg| analyzer.to_daft_expr(arg))
            .collect::<ConnectResult<Vec<_>>>()?;

        let [input, base] = args.as_slice() else {
            invalid_argument_err!("log requires exactly 2 arguments");
        };

        let base = match base.as_ref().as_literal() {
            Some(LiteralValue::Int8(i)) => *i as f64,
            Some(LiteralValue::UInt8(u)) => *u as f64,
            Some(LiteralValue::Int16(i)) => *i as f64,
            Some(LiteralValue::UInt16(u)) => *u as f64,
            Some(LiteralValue::Int32(i)) => *i as f64,
            Some(LiteralValue::UInt32(u)) => *u as f64,
            Some(LiteralValue::Int64(i)) => *i as f64,
            Some(LiteralValue::UInt64(u)) => *u as f64,
            Some(LiteralValue::Float64(f)) => *f,
            _ => invalid_argument_err!("log base must be a number"),
        };
        Ok(log(input.clone(), base))
    }
}

struct RoundFunction;

impl SparkFunction for RoundFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        let mut args = args
            .iter()
            .map(|arg| analyzer.to_daft_expr(arg))
            .collect::<ConnectResult<Vec<_>>>()?
            .into_iter();

        let input = args
            .next()
            .ok_or_else(|| ConnectError::invalid_argument("Expected 1 input arg, got 0"))?;

        let scale = match args.next().as_ref().and_then(|e| e.as_literal()) {
            Some(LiteralValue::Int8(i)) => Some(*i as i32),
            Some(LiteralValue::UInt8(u)) => Some(*u as i32),
            Some(LiteralValue::Int16(i)) => Some(*i as i32),
            Some(LiteralValue::UInt16(u)) => Some(*u as i32),
            Some(LiteralValue::Int32(i)) => Some(*i),
            Some(LiteralValue::UInt32(u)) => Some(*u as i32),
            Some(LiteralValue::Int64(i)) => Some(*i as i32),
            Some(LiteralValue::UInt64(u)) => Some(*u as i32),
            None => None,
            _ => invalid_argument_err!("round precision must be an integer"),
        };

        Ok(round(input, scale))
    }
}
