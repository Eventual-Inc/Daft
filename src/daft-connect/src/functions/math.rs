use daft_dsl::functions::ScalarFunction;
use daft_functions::numeric::{
    abs::Abs,
    cbrt::Cbrt,
    ceil::Ceil,
    exp::{Exp, Expm1},
    floor::Floor,
    log::{log, Ln, Log10, Log1p, Log2},
    round::Round,
    sign::{Negative, Sign},
    sqrt::Sqrt,
    trigonometry::{
        ArcCos, ArcCosh, ArcSin, ArcSinh, ArcTan, ArcTanh, Atan2, Cos, Cosh, Cot, Csc, Degrees,
        Radians, Sec, Sin, Sinh, Tan, Tanh,
    },
};
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction};
use crate::{
    error::ConnectResult, invalid_argument_err, spark_analyzer::expr_analyzer::analyze_expr,
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
        parent.add_todo_fn("bin");
        parent.add_fn("cbrt", Cbrt {});
        parent.add_fn("ceil", Ceil {});
        parent.add_fn("ceiling", Ceil {});
        parent.add_todo_fn("conv");
        parent.add_fn("cos", Cos {});
        parent.add_fn("cosh", Cosh {});
        parent.add_fn("cot", Cot {});
        parent.add_fn("csc", Csc {});
        parent.add_todo_fn("e");
        parent.add_fn("exp", Exp {});
        parent.add_fn("expm1", Expm1 {});
        parent.add_todo_fn("factorial");
        parent.add_fn("floor", Floor {});
        parent.add_todo_fn("hex");
        parent.add_todo_fn("unhex");
        parent.add_todo_fn("hypot");
        parent.add_fn("ln", Ln {});
        parent.add_fn("log", LogFunction {});
        parent.add_fn("log10", Log10 {});
        parent.add_fn("log1p", Log1p {});
        parent.add_fn("log2", Log2 {});
        parent.add_fn("negate", Negative {});
        parent.add_fn("negative", Negative {});
        parent.add_todo_fn("pi");
        parent.add_todo_fn("pmod");
        parent.add_todo_fn("pow");
        parent.add_todo_fn("power");
        parent.add_todo_fn("rint");
        parent.add_fn("round", RoundFunction);
        parent.add_todo_fn("bround");
        parent.add_fn("sec", Sec {});
        parent.add_todo_fn("shiftleft");
        parent.add_todo_fn("shiftright");
        parent.add_fn("sign", Sign {});
        parent.add_fn("signum", Sign {});
        parent.add_fn("sin", Sin {});
        parent.add_fn("sinh", Sinh {});
        parent.add_fn("tan", Tan {});
        parent.add_fn("tanh", Tanh {});
        parent.add_todo_fn("toDegrees");
        parent.add_todo_fn("try_add");
        parent.add_todo_fn("try_avg");
        parent.add_todo_fn("try_divide");
        parent.add_todo_fn("try_multiply");
        parent.add_todo_fn("try_subtract");
        parent.add_todo_fn("try_sum");
        parent.add_todo_fn("try_to_binary");
        parent.add_todo_fn("try_to_number");
        parent.add_fn("degrees", Degrees {});
        parent.add_todo_fn("toRadians");
        parent.add_fn("radians", Radians {});
        parent.add_todo_fn("width_bucket");
        //
    }
}

struct LogFunction;
impl SparkFunction for LogFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
            .collect::<ConnectResult<Vec<_>>>()?;

        let [input, base] = args.as_slice() else {
            invalid_argument_err!("log requires exactly 2 arguments");
        };

        Ok(log(input.clone(), base.clone()))
    }
}

struct RoundFunction;

impl SparkFunction for RoundFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
            .collect::<ConnectResult<Vec<_>>>()?;

        Ok(ScalarFunction::new(Round, args).into())
    }
}
