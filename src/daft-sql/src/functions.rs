use std::{collections::HashMap, sync::Arc};

use daft_dsl::ExprRef;
use hashing::SQLModuleHashing;
use once_cell::sync::Lazy;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments,
};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    modules::{
        hashing, SQLModule, SQLModuleAggs, SQLModuleConfig, SQLModuleFloat, SQLModuleImage,
        SQLModuleJson, SQLModuleList, SQLModuleMap, SQLModuleNumeric, SQLModulePartitioning,
        SQLModulePython, SQLModuleSketch, SQLModuleStructs, SQLModuleTemporal, SQLModuleUtf8,
    },
    planner::SQLPlanner,
    unsupported_sql_err,
};

/// [SQL_FUNCTIONS] is a singleton that holds all the registered SQL functions.
pub(crate) static SQL_FUNCTIONS: Lazy<SQLFunctions> = Lazy::new(|| {
    let mut functions = SQLFunctions::new();
    functions.register::<SQLModuleAggs>();
    functions.register::<SQLModuleFloat>();
    functions.register::<SQLModuleHashing>();
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
    functions.register::<SQLModuleConfig>();
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
pub trait SQLFunction: Send + Sync {
    /// helper function to extract the function args into a list of [ExprRef]
    /// This is used to convert unnamed arguments into [ExprRef]s.
    /// ```sql
    /// SELECT concat('hello', 'world');
    /// ```
    /// this  will be converted to: [lit("hello"), lit("world")]
    ///
    /// Using this on a function with named arguments will result in an error.
    /// ```sql
    /// SELECT concat(arg1 => 'hello', arg2 => 'world');
    /// ```
    fn args_to_expr_unnamed(
        &self,
        inputs: &[FunctionArg],
        planner: &SQLPlanner,
    ) -> SQLPlannerResult<Vec<ExprRef>> {
        inputs
            .iter()
            .map(|arg| planner.plan_function_arg(arg))
            .collect::<SQLPlannerResult<Vec<_>>>()
    }

    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef>;

    /// Produce the docstrings for this SQL function, parametrized by an alias which is the function name to invoke this in SQL
    fn docstrings(&self, alias: &str) -> String {
        format!("{alias}: No docstring available")
    }

    /// Produce the docstrings for this SQL function, parametrized by an alias which is the function name to invoke this in SQL
    fn arg_names(&self) -> &'static [&'static str] {
        &["todo"]
    }
}

/// TODOs
///   - Use multimap for function variants.
///   - Add more functions..
pub struct SQLFunctions {
    pub(crate) map: HashMap<String, Arc<dyn SQLFunction>>,
    pub(crate) docsmap: HashMap<String, (String, &'static [&'static str])>,
}

pub(crate) struct SQLFunctionArguments {
    pub positional: HashMap<usize, ExprRef>,
    pub named: HashMap<String, ExprRef>,
}

impl SQLFunctionArguments {
    pub fn get_positional(&self, idx: usize) -> Option<&ExprRef> {
        self.positional.get(&idx)
    }
    pub fn get_named(&self, name: &str) -> Option<&ExprRef> {
        self.named.get(name)
    }

    pub fn try_get_named<T: SQLLiteral>(&self, name: &str) -> Result<Option<T>, PlannerError> {
        self.named
            .get(name)
            .map(|expr| T::from_expr(expr))
            .transpose()
    }
    pub fn try_get_positional<T: SQLLiteral>(&self, idx: usize) -> Result<Option<T>, PlannerError> {
        self.positional
            .get(&idx)
            .map(|expr| T::from_expr(expr))
            .transpose()
    }
}

pub trait SQLLiteral {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized;
}

impl SQLLiteral for String {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized,
    {
        let e = expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| PlannerError::invalid_operation("Expected a string literal"))?;
        Ok(e.to_string())
    }
}

impl SQLLiteral for char {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized,
    {
        expr.as_literal()
            .and_then(|lit| lit.as_str())
            .and_then(|s| s.chars().next())
            .ok_or_else(|| PlannerError::invalid_operation("Expected a single char literal"))
    }
}

impl SQLLiteral for i64 {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized,
    {
        expr.as_literal()
            .and_then(daft_dsl::LiteralValue::as_i64)
            .ok_or_else(|| PlannerError::invalid_operation("Expected an integer literal"))
    }
}

impl SQLLiteral for usize {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized,
    {
        expr.as_literal()
            .and_then(|lit| lit.as_i64().map(|v| v as Self))
            .ok_or_else(|| PlannerError::invalid_operation("Expected an integer literal"))
    }
}

impl SQLLiteral for bool {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError>
    where
        Self: Sized,
    {
        expr.as_literal()
            .and_then(daft_dsl::LiteralValue::as_bool)
            .ok_or_else(|| PlannerError::invalid_operation("Expected a boolean literal"))
    }
}

impl SQLFunctions {
    /// Create a new [SQLFunctions] instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            docsmap: HashMap::new(),
        }
    }

    /// Register the module to the [SQLFunctions] instance.
    pub fn register<M: SQLModule>(&mut self) {
        M::register(self);
    }

    /// Add a [FunctionExpr] to the [SQLFunctions] instance.
    pub fn add_fn<F: SQLFunction + 'static>(&mut self, name: &str, func: F) {
        self.docsmap
            .insert(name.to_string(), (func.docstrings(name), func.arg_names()));
        self.map.insert(name.to_string(), Arc::new(func));
    }

    /// Get a function by name from the [SQLFunctions] instance.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SQLFunction>> {
        self.map.get(name)
    }
}

impl Default for SQLFunctions {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> SQLPlanner<'a> {
    pub(crate) fn plan_function(&self, func: &Function) -> SQLPlannerResult<ExprRef> {
        // assert using only supported features
        check_features(func)?;

        // lookup function variant(s) by name
        let fns = &SQL_FUNCTIONS;
        // SQL function names are case-insensitive
        let fn_name = func.name.to_string().to_lowercase();
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
                args.args.clone()
            }
        };

        // validate input argument arity and return the validated expression.
        fn_match.to_expr(&args, self)
    }

    pub(crate) fn plan_function_args<T>(
        &self,
        args: &[FunctionArg],
        expected_named: &'static [&'static str],
        expected_positional: usize,
    ) -> SQLPlannerResult<T>
    where
        T: TryFrom<SQLFunctionArguments, Error = PlannerError>,
    {
        self.parse_function_args(args, expected_named, expected_positional)?
            .try_into()
    }
    pub(crate) fn parse_function_args(
        &self,
        args: &[FunctionArg],
        expected_named: &'static [&'static str],
        expected_positional: usize,
    ) -> SQLPlannerResult<SQLFunctionArguments> {
        let mut positional_args = HashMap::new();
        let mut named_args = HashMap::new();
        for (idx, arg) in args.iter().enumerate() {
            match arg {
                // Supporting right arrow or assignment (for backward compatibility) operator for named notation
                // https://www.postgresql.org/docs/current/sql-syntax-calling-funcs.html#SQL-SYNTAX-CALLING-FUNCS-NAMED
                FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::RightArrow | FunctionArgOperator::Assignment,
                } => {
                    if !expected_named.contains(&name.value.as_str()) {
                        unsupported_sql_err!("unexpected named argument: {}", name);
                    }
                    named_args.insert(name.to_string(), self.try_unwrap_function_arg_expr(arg)?);
                }
                FunctionArg::Unnamed(arg) => {
                    if idx >= expected_positional {
                        unsupported_sql_err!("unexpected unnamed argument");
                    }
                    positional_args.insert(idx, self.try_unwrap_function_arg_expr(arg)?);
                }
                other => unsupported_sql_err!("unsupported function argument type: {other}, valid function arguments for this function are: {expected_named:?}."),
            }
        }

        Ok(SQLFunctionArguments {
            positional: positional_args,
            named: named_args,
        })
    }

    pub(crate) fn plan_function_arg(
        &self,
        function_arg: &FunctionArg,
    ) -> SQLPlannerResult<ExprRef> {
        match function_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.plan_expr(expr),
            _ => unsupported_sql_err!("named function args not yet supported"),
        }
    }

    pub(crate) fn try_unwrap_function_arg_expr(
        &self,
        expr: &FunctionArgExpr,
    ) -> SQLPlannerResult<ExprRef> {
        match expr {
            FunctionArgExpr::Expr(expr) => self.plan_expr(expr),
            _ => unsupported_sql_err!("Wildcard function args not yet supported"),
        }
    }
}
