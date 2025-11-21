use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use daft_core::prelude::Schema;
use daft_dsl::{
    Expr, ExprRef, Operator, WindowExpr, WindowSpec, binary_op,
    expr::window::{WindowBoundary, WindowFrame},
    functions::{
        BuiltinScalarFn, BuiltinScalarFnVariant, FUNCTION_REGISTRY, FunctionArgs, ScalarUDF,
    },
    unresolved_col,
};
use daft_session::Session;
use sqlparser::ast::{
    DuplicateTreatment, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator,
    FunctionArguments, WindowType,
};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err,
    modules::{
        SQLModule, SQLModuleAggs, SQLModuleConfig, SQLModuleMap, SQLModulePartitioning,
        SQLModulePython, SQLModuleSketch, SQLModuleStructs, SQLModuleWindow,
    },
    planner::SQLPlanner,
    unsupported_sql_err,
};
pub struct SQLElement;

impl SQLFunction for SQLElement {
    fn to_expr(&self, _: &[FunctionArg], _: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        Ok(unresolved_col(""))
    }
}
pub struct SQLConcat;

impl SQLFunction for SQLConcat {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let inputs = inputs
            .iter()
            .map(|input| planner.plan_function_arg(input).map(|arg| arg.into_inner()))
            .collect::<SQLPlannerResult<Vec<_>>>()?;
        let mut inputs = inputs.into_iter();

        let Some(mut first) = inputs.next() else {
            invalid_operation_err!("concat requires at least one argument")
        };
        for input in inputs {
            first = binary_op(Operator::Plus, first, input);
        }

        Ok(first)
    }

    fn docstrings(&self, _: &str) -> String {
        "Concatenate the inputs into a single string".to_string()
    }
}

/// [SQL_FUNCTIONS] is a singleton that holds all the registered SQL functions.
pub(crate) static SQL_FUNCTIONS: LazyLock<SQLFunctions> = LazyLock::new(|| {
    let mut functions = SQLFunctions::new();
    functions.register::<SQLModuleAggs>();
    functions.register::<SQLModuleMap>();
    functions.register::<SQLModulePartitioning>();
    functions.register::<SQLModulePython>();
    functions.register::<SQLModuleSketch>();
    functions.register::<SQLModuleStructs>();
    functions.register::<SQLModuleConfig>();
    functions.register::<SQLModuleWindow>();
    functions.add_fn("concat", SQLConcat);
    functions.add_fn("element", SQLElement);
    for (name, function_factory) in FUNCTION_REGISTRY.read().unwrap().entries() {
        // Note:
        //  FunctionModule came from SQLModule, but SQLModule still remains.
        //  We must add all functions from the registry to the SQLModule, but
        //  now the FunctionModule has the ability to represent both logical
        //  and physical via ScalarFunctionFactory. This is an easy migration
        //  because, like the python API, we've only had dynamic functions on
        //  the SQL side. The solution is to add all `DynamicScalarFunction`
        //  by calling get_function with empty arguments and only adding the ok's.
        let args = FunctionArgs::empty();
        let schema = Schema::empty();
        if let Ok(function) = function_factory.get_function(args, &schema) {
            functions.add_fn(name, function);
        }
    }
    functions
});

impl SQLFunction for Arc<dyn ScalarUDF> {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        let inputs = inputs
            .iter()
            .map(|input| planner.plan_function_arg(input))
            .collect::<SQLPlannerResult<Vec<_>>>()?;
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(self.clone()),
            inputs: daft_dsl::functions::FunctionArgs::try_new(inputs)?,
        }
        .into())
    }
    fn docstrings(&self, _alias: &str) -> String {
        ScalarUDF::docstring(self.as_ref()).to_string()
    }
}

impl SQLFunction for BuiltinScalarFnVariant {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        let inputs = inputs
            .iter()
            .map(|input| planner.plan_function_arg(input))
            .collect::<SQLPlannerResult<Vec<_>>>()?;

        Ok(BuiltinScalarFn {
            func: self.clone(),
            inputs: daft_dsl::functions::FunctionArgs::try_new(inputs)?,
        }
        .into())
    }
}

/// Current feature-set
fn check_features(func: &Function) -> SQLPlannerResult<()> {
    if func.filter.is_some() {
        // <agg>(..) FILTER (WHERE ..)
        unsupported_sql_err!("Aggregation `FILTER`");
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
    ) -> SQLPlannerResult<daft_dsl::functions::FunctionArgs<ExprRef>> {
        let inputs = inputs
            .iter()
            .map(|arg| planner.plan_function_arg(arg))
            .collect::<SQLPlannerResult<Vec<_>>>()?;

        Ok(daft_dsl::functions::FunctionArgs::try_new(inputs)?)
    }

    // nit cleanup: argument consistency with SQLTableFunction
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

pub struct DeprecatedSQLFunction {
    pub name: &'static str,
    pub replacement: &'static str,
    pub function: &'static dyn SQLFunction,
}

impl SQLFunction for DeprecatedSQLFunction {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        eprintln!(
            "WARNING: `{}` function is deprecated. Use `{}` instead.",
            self.name, self.replacement
        );
        self.function.to_expr(inputs, planner)
    }

    fn docstrings(&self, alias: &str) -> String {
        self.function.docstrings(alias)
    }

    fn arg_names(&self) -> &'static [&'static str] {
        self.function.arg_names()
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
            .and_then(daft_core::lit::Literal::as_i64)
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
            .and_then(daft_core::lit::Literal::as_bool)
            .ok_or_else(|| PlannerError::invalid_operation("Expected a boolean literal"))
    }
}

impl<T: SQLLiteral> SQLLiteral for Vec<T> {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError> {
        expr.as_list()
            .map(|items| items.iter().map(T::from_expr).collect())
            .unwrap_or_else(|| unsupported_sql_err!("Expected a list literal"))
    }
}

impl<T: SQLLiteral> SQLLiteral for HashMap<String, T> {
    fn from_expr(expr: &ExprRef) -> Result<Self, PlannerError> {
        // get reference to struct
        let fields = expr
            .as_literal()
            .and_then(daft_core::lit::Literal::as_struct)
            .ok_or_else(|| PlannerError::invalid_operation("Expected a struct literal"))?;
        // add literals to new map
        let mut map = Self::new();
        for (key, lit) in fields {
            let e = Expr::Literal(lit.clone()).arced();
            let k = key.clone();
            let v = T::from_expr(&e)?;
            map.insert(k, v);
        }
        Ok(map)
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

impl SQLPlanner<'_> {
    pub(crate) fn plan_function(&self, func: &Function) -> SQLPlannerResult<ExprRef> {
        // assert using only supported features
        check_features(func)?;

        fn get_func_from_sqlfunctions_registry(
            name: impl AsRef<str>,
        ) -> Option<Arc<dyn SQLFunction>> {
            let name = name.as_ref();
            SQL_FUNCTIONS.get(name).cloned()
        }

        fn get_func_from_session(
            session: &Session,
            name: impl AsRef<str>,
        ) -> SQLPlannerResult<Option<Arc<dyn SQLFunction>>> {
            let name = name.as_ref();

            match session.get_function(name)? {
                Some(f) => Ok(Some(Arc::new(f))),
                None => Ok(None),
            }
        }

        // lookup function variant(s) by name
        // SQL function names are case-insensitive
        let fn_name = func.name.to_string().to_lowercase();

        let mut fn_match = if let Some(fn_match) = get_func_from_session(self.session(), &fn_name)?
        {
            fn_match
        } else {
            get_func_from_sqlfunctions_registry(fn_name.as_str()).ok_or_else(|| {
                PlannerError::unsupported_sql(format!("Function `{}` not found", fn_name))
            })?
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
                let duplicate_treatment =
                    args.duplicate_treatment.unwrap_or(DuplicateTreatment::All);

                match (fn_name.as_str(), duplicate_treatment) {
                    ("count", DuplicateTreatment::Distinct) => {
                        fn_match = get_func_from_sqlfunctions_registry("count_distinct")
                            .ok_or_else(|| {
                                PlannerError::unsupported_sql(format!(
                                    "Function `{}` not found",
                                    fn_name
                                ))
                            })?;
                    }
                    ("count", DuplicateTreatment::All) => (),
                    (name, DuplicateTreatment::Distinct) => {
                        unsupported_sql_err!("DISTINCT is only supported on COUNT, not on {}", name)
                    }
                    (_, DuplicateTreatment::All) => (),
                }

                if !args.clauses.is_empty() {
                    unsupported_sql_err!("function arguments with clauses");
                }
                args.args.clone()
            }
        };

        if func.over.is_some() {
            let window_spec = Arc::new(self.parse_window_spec(func.over.as_ref().unwrap())?);
            let window_fn = fn_match.to_expr(&args, self)?;
            Ok(match &*window_fn {
                Expr::Agg(agg_expr) => {
                    Expr::Over(WindowExpr::Agg(agg_expr.clone()), window_spec).arced()
                }
                Expr::WindowFunction(window_expr) => {
                    Expr::Over(window_expr.clone(), window_spec).arced()
                }
                _ => unsupported_sql_err!("window function expected"),
            })
        } else {
            // validate input argument arity and return the validated expression.
            fn_match.to_expr(&args, self)
        }
    }

    fn parse_window_spec(&self, over: &WindowType) -> SQLPlannerResult<WindowSpec> {
        match over {
            WindowType::WindowSpec(spec) => {
                let mut window_spec = WindowSpec::default();

                if !spec.partition_by.is_empty() {
                    for expr in &spec.partition_by {
                        let parsed_expr = self.plan_expr(expr)?;
                        window_spec.partition_by.push(parsed_expr);
                    }
                }

                if !spec.order_by.is_empty() {
                    for order in &spec.order_by {
                        let parsed_expr = self.plan_expr(&order.expr)?;
                        window_spec.order_by.push(parsed_expr);
                        window_spec
                            .descending
                            .push(order.asc.is_some_and(|asc| !asc));
                        window_spec.nulls_first.push(
                            order
                                .nulls_first
                                .unwrap_or_else(|| order.asc.is_some_and(|asc| !asc)),
                        );
                    }
                }

                if spec.window_frame.is_some()
                    && let Some(sql_frame) = &spec.window_frame
                {
                    let is_range_frame =
                        matches!(sql_frame.units, sqlparser::ast::WindowFrameUnits::Range);

                    let start =
                        self.convert_window_frame_bound(&sql_frame.start_bound, is_range_frame)?;

                    // Convert end bound or default to CURRENT ROW if not specified
                    let end = match &sql_frame.end_bound {
                        Some(end_bound) => {
                            self.convert_window_frame_bound(end_bound, is_range_frame)?
                        }
                        None => WindowBoundary::Offset(0), // CURRENT ROW
                    };

                    window_spec.frame = Some(WindowFrame { start, end });
                }

                if let Some(current_plan) = &self.current_plan {
                    window_spec = current_plan.resolve_window_spec(window_spec)?;
                }

                Ok(window_spec)
            }
            WindowType::NamedWindow(_) => {
                unsupported_sql_err!("Named windows are not supported yet")
            }
        }
    }

    /// Helper function to convert SQLParser's WindowFrameBound to Daft's WindowBoundary
    fn convert_window_frame_bound(
        &self,
        bound: &sqlparser::ast::WindowFrameBound,
        is_range_frame: bool,
    ) -> SQLPlannerResult<daft_dsl::expr::window::WindowBoundary> {
        use daft_dsl::expr::window::WindowBoundary;

        match bound {
            sqlparser::ast::WindowFrameBound::CurrentRow => Ok(WindowBoundary::Offset(0)),
            sqlparser::ast::WindowFrameBound::Preceding(None) => {
                Ok(WindowBoundary::UnboundedPreceding)
            }
            sqlparser::ast::WindowFrameBound::Following(None) => {
                Ok(WindowBoundary::UnboundedFollowing)
            }
            sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => {
                let parsed_expr = self.plan_expr(expr)?;

                if let Some(lit) = parsed_expr.as_literal() {
                    if is_range_frame {
                        Ok(WindowBoundary::RangeOffset(lit.neg()?))
                    } else if let Some(value) = lit.as_i64() {
                        Ok(WindowBoundary::Offset(-value))
                    } else {
                        unsupported_sql_err!(
                            "Window frame bound must be an integer for ROWS frames, found: {}",
                            lit
                        )
                    }
                } else {
                    unsupported_sql_err!(
                        "Window frame bound must be a literal, found: {}",
                        parsed_expr
                    )
                }
            }
            sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
                let parsed_expr = self.plan_expr(expr)?;

                if let Some(lit) = parsed_expr.as_literal() {
                    if is_range_frame {
                        Ok(WindowBoundary::RangeOffset(lit.clone()))
                    } else if let Some(value) = lit.as_i64() {
                        Ok(WindowBoundary::Offset(value))
                    } else {
                        unsupported_sql_err!(
                            "Window frame bound must be an integer for ROWS frames, found: {}",
                            lit
                        )
                    }
                } else {
                    unsupported_sql_err!(
                        "Window frame bound must be a literal, found: {}",
                        parsed_expr
                    )
                }
            }
        }
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
                other => unsupported_sql_err!(
                    "unsupported function argument type: {other}, valid function arguments for this function are: {expected_named:?}."
                ),
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
    ) -> SQLPlannerResult<daft_dsl::functions::FunctionArg<ExprRef>> {
        match function_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(
                daft_dsl::functions::FunctionArg::unnamed(self.plan_expr(expr)?),
            ),
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expr),
                operator: _,
            } => {
                let expr = self.plan_expr(expr)?;
                Ok(daft_dsl::functions::FunctionArg::named(
                    name.to_string(),
                    expr,
                ))
            }
            _ => unsupported_sql_err!("non expr args not yet supported"),
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

/// A namespace for function argument parsing helpers.
pub(crate) mod args {
    use common_io_config::IOConfig;

    use super::SQLFunctionArguments;
    use crate::{error::PlannerError, modules::config::expr_to_iocfg};

    /// Parses io_config which is used in several SQL functions.
    pub(crate) fn parse_io_config(args: &SQLFunctionArguments) -> Result<IOConfig, PlannerError> {
        args.get_named("io_config")
            .map(expr_to_iocfg)
            .transpose()
            .map(|op| op.unwrap_or_default())
    }
}
