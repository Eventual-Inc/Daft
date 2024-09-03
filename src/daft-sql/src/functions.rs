use std::collections::HashMap;

use daft_dsl::{functions::FunctionExpr, AggExpr, ExprRef};
use once_cell::sync::Lazy;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{error::SQLPlannerResult, modules::*, planner::SQLPlanner, unsupported_sql_err};

/// [SQL_FUNCTIONS] is a singleton that holds all the registered SQL functions.
static SQL_FUNCTIONS: Lazy<SQLFunctions> = Lazy::new(|| {
    let mut functions = SQLFunctions::new();
    functions.register::<SQLModuleAggs>();
    functions.register::<SQLModuleFloat>();
    functions.register::<SQLModuleImage>();
    functions.register::<SQLModuleJson>();
    functions.register::<SQLModuleList>();
    functions.register::<SQLModuleMap>();
    functions.register::<SQLModuleNumeric>();
    functions.register::<SQLModulePartitioning>();
    functions.register::<SQLModulePython>();
    functions.register::<SQLModuleSketch>();
    functions.register::<SQLModuleStructs>();
    functions.register::<SQLModuleTemporal>();
    functions.register::<SQLModuleUtf8>();
    functions
});

/// Current feature-set
fn check_features(func: &Function) -> SQLPlannerResult<()> {
    if func.filter.is_some() {
        // <agg>(..) FILTER (WHERE ..)
        unsupported_sql_err!("Aggregation `FILTER`");
    }
    if func.over.is_some() {
        // WINDOW functions
        unsupported_sql_err!("Window functions `OVER`");
    }
    if !func.within_group.is_empty() {
        // <agg>(...) WITHIN GROUP
        unsupported_sql_err!("Aggregation `WITHIN GROUP`");
    }
    if func.null_treatment.is_some() {
        // Snowflake NULL treatment
        unsupported_sql_err!("Window function `IGNORE|RESPECT NULLS`");
    }
    if func.parameters != FunctionArguments::None {
        // ClickHouse parametric functions
        unsupported_sql_err!("Parameterized function calls");
    }
    Ok(())
}

/// [SQLFunction] extends [FunctionExpr] with basic input validation (arity-check).
///
/// TODOs
///  - Support for ScalarUDF as either another enum variant or make SQLFunction a trait.
pub enum SQLFunction {
    Function(FunctionExpr),
    Agg(AggExpr),
}

/// TODOs
///   - Use multimap for function variants.
///   - Add more functions..
pub struct SQLFunctions {
    map: HashMap<String, SQLFunction>,
}

/// Adds a `to_expr` method to [SQLFunction] to convert it to an [ExprRef].
///
/// TODOs
///   - Consider splitting input validation from creating an [ExprRef].
///   - Consider extending ScalarUDF
impl SQLFunction {
    /// Convert the [SQLFunction] to an [ExprRef].
    fn to_expr(&self, inputs: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
        match self {
            SQLFunction::Function(func) => {
                use FunctionExpr::*;
                match func {
                    Numeric(expr) => numeric::to_expr(expr, inputs),
                    Utf8(expr) => utf8::to_expr(expr, inputs),
                    Temporal(_) => unsupported_sql_err!("Temporal functions"),
                    List(_) => unsupported_sql_err!("List functions"),
                    Map(_) => unsupported_sql_err!("Map functions"),
                    Sketch(_) => unsupported_sql_err!("Sketch functions"),
                    Struct(_) => unsupported_sql_err!("Struct functions"),
                    Json(_) => unsupported_sql_err!("Json functions"),
                    Image(_) => unsupported_sql_err!("Image functions"),
                    Python(_) => unsupported_sql_err!("Python functions"),
                    Partitioning(_) => unsupported_sql_err!("Partitioning functions"),
                }
            }
            SQLFunction::Agg(expr) => aggs::to_expr(expr, inputs),
        }
    }
}

impl SQLFunctions {
    /// Create a new [SQLFunctions] instance.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Register the module to the [SQLFunctions] instance.
    pub fn register<M: SQLModule>(&mut self) {
        M::register(self);
    }

    /// Add a [FunctionExpr] to the [SQLFunctions] instance.
    pub fn add_fn(&mut self, name: &str, func: FunctionExpr) {
        self.map
            .insert(name.to_string(), SQLFunction::Function(func));
    }

    /// Add an [AggExpr] to the [SQLFunctions] instance.
    pub fn add_agg(&mut self, name: &str, agg: AggExpr) {
        self.map.insert(name.to_string(), SQLFunction::Agg(agg));
    }

    /// Get a function by name from the [SQLFunctions] instance.
    pub fn get(&self, name: &str) -> Option<&SQLFunction> {
        self.map.get(name)
    }
}

impl Default for SQLFunctions {
    fn default() -> Self {
        Self::new()
    }
}

impl SQLPlanner {
    pub(crate) fn plan_function(&self, func: &Function) -> SQLPlannerResult<ExprRef> {
        // assert using only supported features
        check_features(func)?;

        // lookup function variant(s) by name
        let fns = &SQL_FUNCTIONS;
        let fn_name = func.name.to_string();
        let fn_match = match fns.get(&fn_name) {
            Some(func) => func,
            None => unsupported_sql_err!("Function `{}` not found", fn_name),
        };

        // TODO: Filter the variants for correct arity.
        //
        // let argc_expect = fn_match.argc as usize;
        // let argc_actual = match &func.args {
        //     FunctionArguments::None => 0,
        //     FunctionArguments::Subquery(_) => unsupported_sql_err!("subquery"),
        //     FunctionArguments::List(list) => list.args.len(),
        // };
        // ensure!(
        //     argc_expect == argc_actual,
        //     "{} takes exactly {} argument(s), example: {}",
        //     fn_name,
        //     argc_expect,
        //     fn_match.help
        // );

        // recurse on function arguments
        let args = match &func.args {
            sqlparser::ast::FunctionArguments::None => vec![],
            sqlparser::ast::FunctionArguments::Subquery(_) => {
                unsupported_sql_err!("subquery function argument")
            }
            sqlparser::ast::FunctionArguments::List(args) => {
                if args.duplicate_treatment.is_some() {
                    unsupported_sql_err!("function argument with duplicate treatment");
                }
                if !args.clauses.is_empty() {
                    unsupported_sql_err!("function arguments with clauses");
                }
                args.args
                    .iter()
                    .map(|arg| self.plan_function_arg(arg))
                    .collect::<SQLPlannerResult<Vec<_>>>()?
            }
        };

        // validate input argument arity and return the validated expression.
        fn_match.to_expr(args.as_slice())
    }

    fn plan_function_arg(&self, function_arg: &FunctionArg) -> SQLPlannerResult<ExprRef> {
        match function_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.plan_expr(expr),
            _ => unsupported_sql_err!("named function args not yet supported"),
        }
    }
}
