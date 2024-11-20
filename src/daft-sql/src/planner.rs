use std::{
    borrow::Cow,
    cell::{Ref, RefCell, RefMut},
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode},
    has_agg, lit, literals_to_series, null_lit, AggExpr, Expr, ExprRef, LiteralValue, Operator,
    OuterReferenceColumn, Subquery,
};
use daft_functions::{
    numeric::{ceil::ceil, floor::floor},
    utf8::{ilike, like, to_date, to_datetime},
};
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::{
    ast::{
        ArrayElemTypeDef, BinaryOperator, CastKind, DateTimeField, Distinct, ExactNumberInfo,
        ExcludeSelectItem, GroupByExpr, Ident, Query, SelectItem, SetExpr, Statement, StructField,
        Subscript, TableAlias, TableWithJoins, TimezoneInfo, UnaryOperator, Value,
        WildcardAdditionalOptions, With,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

use crate::{
    catalog::SQLCatalog, column_not_found_err, error::*, invalid_operation_err,
    table_not_found_err, unsupported_sql_err,
};

/// A named logical plan
/// This is used to keep track of the table name associated with a logical plan while planning a SQL query
#[derive(Debug, Clone)]
pub struct Relation {
    pub(crate) inner: LogicalPlanBuilder,
    pub(crate) name: String,
    pub(crate) alias: Option<TableAlias>,
}

impl Relation {
    pub fn new(inner: LogicalPlanBuilder, name: String) -> Self {
        Self {
            inner,
            name,
            alias: None,
        }
    }
    pub fn with_alias(self, alias: TableAlias) -> Self {
        Self {
            alias: Some(alias),
            ..self
        }
    }
    pub fn get_name(&self) -> String {
        self.alias
            .as_ref()
            .map(|alias| ident_to_str(&alias.name))
            .unwrap_or_else(|| self.name.clone())
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Context that is shared across a query and its subqueries
struct PlannerContext {
    catalog: SQLCatalog,
    cte_map: HashMap<String, Relation>,
}

impl Default for PlannerContext {
    fn default() -> Self {
        Self {
            catalog: SQLCatalog::new(),
            cte_map: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct SQLPlanner<'a> {
    current_relation: Option<Relation>,
    table_map: HashMap<String, Relation>,
    /// Aliases from selection that can be used in other clauses
    /// but may not yet be in the schema of `current_relation`.
    alias_map: HashMap<String, ExprRef>,
    /// outer query in a subquery
    parent: Option<&'a SQLPlanner<'a>>,
    context: Rc<RefCell<PlannerContext>>,
}

impl<'a> SQLPlanner<'a> {
    pub fn new(catalog: SQLCatalog) -> Self {
        let context = Rc::new(RefCell::new(PlannerContext {
            catalog,
            ..Default::default()
        }));

        Self {
            context,
            ..Default::default()
        }
    }

    fn new_child(&'a self) -> Self {
        Self {
            context: self.context.clone(),
            parent: Some(self),
            ..Default::default()
        }
    }

    fn new_with_context(&'a self) -> Self {
        Self {
            context: self.context.clone(),
            ..Default::default()
        }
    }

    /// SAFETY: it is up to the caller to ensure that the relation is set before calling this method.
    /// It's a programming error to call this method without setting the relation first.
    /// Some methods such as `plan_expr` do not require the relation to be set.
    fn relation_mut(&mut self) -> &mut Relation {
        self.current_relation.as_mut().expect("relation not set")
    }

    pub(crate) fn relation_opt(&self) -> Option<&Relation> {
        self.current_relation.as_ref()
    }

    fn context_mut(&self) -> RefMut<'_, PlannerContext> {
        self.context.as_ref().borrow_mut()
    }

    fn cte_map(&self) -> Ref<'_, HashMap<String, Relation>> {
        Ref::map(self.context.borrow(), |i| &i.cte_map)
    }

    fn catalog(&self) -> Ref<'_, SQLCatalog> {
        Ref::map(self.context.borrow(), |i| &i.catalog)
    }

    /// Clears the current context used for planning a SQL query
    fn clear_context(&mut self) {
        self.current_relation = None;
        self.table_map.clear();
        self.context_mut().cte_map.clear();
    }

    fn register_cte(&self, mut rel: Relation, column_aliases: &[Ident]) -> SQLPlannerResult<()> {
        if !column_aliases.is_empty() {
            let schema = rel.schema();
            let columns = schema.names();
            if columns.len() != column_aliases.len() {
                invalid_operation_err!(
                    "Column count mismatch: expected {} columns, found {}",
                    column_aliases.len(),
                    columns.len()
                );
            }

            let projection = columns
                .into_iter()
                .zip(column_aliases)
                .map(|(name, alias)| col(name).alias(ident_to_str(alias)))
                .collect::<Vec<_>>();

            rel.inner = rel.inner.select(projection)?;
        }
        self.context_mut().cte_map.insert(rel.get_name(), rel);
        Ok(())
    }

    fn plan_ctes(&self, with: &With) -> SQLPlannerResult<()> {
        if with.recursive {
            unsupported_sql_err!("Recursive CTEs are not supported");
        }

        for cte in &with.cte_tables {
            if cte.materialized.is_some() {
                unsupported_sql_err!("MATERIALIZED is not supported");
            }

            if cte.from.is_some() {
                invalid_operation_err!("FROM should only exist in recursive CTEs");
            }

            let name = ident_to_str(&cte.alias.name);
            let plan = self.new_with_context().plan_query(&cte.query)?;
            let rel = Relation::new(plan, name);

            self.register_cte(rel, cte.alias.columns.as_slice())?;
        }
        Ok(())
    }

    pub fn plan_sql(&mut self, sql: &str) -> SQLPlannerResult<LogicalPlanRef> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize()?;

        let mut parser = Parser::new(&GenericDialect {})
            .with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            })
            .with_tokens(tokens);

        let statements = parser.parse_statements()?;

        let plan = match statements.len() {
            1 => Ok(self.plan_statement(&statements[0])?),
            other => {
                unsupported_sql_err!("Only exactly one SQL statement allowed, found {}", other)
            }
        };
        self.clear_context();
        plan
    }

    fn plan_statement(&mut self, statement: &Statement) -> SQLPlannerResult<LogicalPlanRef> {
        match statement {
            Statement::Query(query) => Ok(self.plan_query(query)?.build()),
            other => unsupported_sql_err!("{}", other),
        }
    }

    fn plan_query(&mut self, query: &Query) -> SQLPlannerResult<LogicalPlanBuilder> {
        check_query_features(query)?;

        let selection = match query.body.as_ref() {
            SetExpr::Select(selection) => selection,
            SetExpr::Query(_) => unsupported_sql_err!("Subqueries are not supported"),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                use sqlparser::ast::{
                    SetOperator::{Intersect, Union},
                    SetQuantifier,
                };
                fn make_query(expr: &SetExpr) -> Query {
                    Query {
                        with: None,
                        body: Box::new(expr.clone()),
                        order_by: None,
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    }
                }

                let left = self.new_with_context().plan_query(&make_query(left))?;
                let right = self.new_with_context().plan_query(&make_query(right))?;

                return match (op, set_quantifier) {
                    (Union, SetQuantifier::All) => left.union(&right, true).map_err(|e| e.into()),

                    (Union, SetQuantifier::None | SetQuantifier::Distinct) => {
                        left.union(&right, false).map_err(|e| e.into())
                    }

                    (Intersect, SetQuantifier::All) => {
                        left.intersect(&right, true).map_err(|e| e.into())
                    }
                    (Intersect, SetQuantifier::None | SetQuantifier::Distinct) => {
                        left.intersect(&right, false).map_err(|e| e.into())
                    }
                    (op, set_quantifier) => {
                        unsupported_sql_err!("{op} {set_quantifier} is not supported.")
                    }
                };
            }
            SetExpr::Values(..) => unsupported_sql_err!("VALUES are not supported"),
            SetExpr::Insert(..) => unsupported_sql_err!("INSERT is not supported"),
            SetExpr::Update(..) => unsupported_sql_err!("UPDATE is not supported"),
            SetExpr::Table(..) => unsupported_sql_err!("TABLE is not supported"),
        };
        check_select_features(selection)?;

        if let Some(with) = &query.with {
            self.plan_ctes(with)?;
        }

        // FROM/JOIN
        let from = selection.clone().from;
        let rel = self.plan_from(&from)?;
        let schema = rel.schema();
        self.current_relation = Some(rel);

        // SELECT
        let mut projections = Vec::with_capacity(selection.projection.len());
        let mut projection_fields = Vec::with_capacity(selection.projection.len());
        for expr in &selection.projection {
            let exprs = self.select_item_to_expr(expr, &schema)?;

            let fields = exprs
                .iter()
                .map(|expr| expr.to_field(&schema).map_err(PlannerError::from))
                .collect::<SQLPlannerResult<Vec<_>>>()?;

            projections.extend(exprs);

            projection_fields.extend(fields);
        }

        let projection_schema = Schema::new(projection_fields)?;
        let has_orderby = query.order_by.is_some();

        // WHERE
        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection)?;
            let rel = self.relation_mut();
            rel.inner = rel.inner.filter(filter)?;
        }

        // GROUP BY
        let mut groupby_exprs = Vec::new();

        match &selection.group_by {
            GroupByExpr::All(s) => {
                if !s.is_empty() {
                    unsupported_sql_err!("GROUP BY ALL");
                }
            }
            GroupByExpr::Expressions(expressions, _) => {
                groupby_exprs = expressions
                    .iter()
                    .map(|expr| self.plan_expr(expr))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
            }
        }

        let has_aggs = projections.iter().any(has_agg) || !groupby_exprs.is_empty();

        if has_aggs {
            let having = selection
                .having
                .as_ref()
                .map(|h| self.plan_expr(h))
                .transpose()?;

            self.plan_aggregate_query(
                &projections,
                &schema,
                has_orderby,
                groupby_exprs,
                query,
                &projection_schema,
                having,
            )?;
        } else {
            self.plan_non_agg_query(projections, schema, has_orderby, query, projection_schema)?;
        }

        match &selection.distinct {
            Some(Distinct::Distinct) => {
                let rel = self.relation_mut();
                rel.inner = rel.inner.distinct()?;
            }
            Some(Distinct::On(_)) => unsupported_sql_err!("DISTINCT ON"),
            None => {}
        }

        if let Some(limit) = &query.limit {
            let limit = self.plan_expr(limit)?;
            if let Expr::Literal(LiteralValue::Int64(limit)) = limit.as_ref() {
                let rel = self.relation_mut();
                rel.inner = rel.inner.limit(*limit, true)?; // TODO: Should this be eager or not?
            } else {
                invalid_operation_err!(
                    "LIMIT <n> must be a constant integer, instead got: {limit}"
                );
            }
        }

        Ok(self.current_relation.clone().unwrap().inner)
    }

    fn plan_non_agg_query(
        &mut self,
        projections: Vec<Arc<Expr>>,
        schema: Arc<Schema>,
        has_orderby: bool,
        query: &Query,
        projection_schema: Schema,
    ) -> Result<(), PlannerError> {
        // Final/selected cols
        // if there is an orderby, and it references a column that is not part of the final projection (such as an alias)
        // then we need to keep the original column in the projection, and remove it at the end
        // ex: `SELECT a as b, c FROM t ORDER BY a`
        // we need to keep a, and c in the first projection
        // but only c in the final projection
        // We only will apply 2 projections if there is an order by and the order by references a column that is not in the final projection
        let mut final_projection = Vec::with_capacity(projections.len());
        let mut orderby_projection = Vec::with_capacity(projections.len());
        for p in &projections {
            let fld = p.to_field(&schema);

            let fld = fld?;
            let name = fld.name.clone();

            // if there is an orderby, then the final projection will only contain the columns that are in the orderby
            final_projection.push(if has_orderby {
                col(name.as_ref())
            } else {
                // otherwise we just do a normal projection
                p.clone()
            });
        }

        if has_orderby {
            let order_by = query.order_by.as_ref().unwrap();

            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            };

            let (orderby_exprs, orderby_desc, orderby_nulls_first) =
                self.plan_order_by_exprs(order_by.exprs.as_slice())?;

            for expr in &orderby_exprs {
                if let Err(DaftError::FieldNotFound(_)) = expr.to_field(&projection_schema) {
                    // this is likely an alias
                    orderby_projection.push(expr.clone());
                }
            }
            // if the orderby references a column that is not in the final projection
            // then we need an additional projection
            let needs_projection = !orderby_projection.is_empty();

            let rel = self.relation_mut();
            if needs_projection {
                let pre_orderby_projections = projections
                    .iter()
                    .cloned()
                    .chain(orderby_projection)
                    .collect::<HashSet<_>>() // dedup
                    .into_iter()
                    .collect::<Vec<_>>();
                rel.inner = rel.inner.select(pre_orderby_projections)?;
            } else {
                rel.inner = rel.inner.select(projections)?;
            }

            rel.inner = rel
                .inner
                .sort(orderby_exprs, orderby_desc, orderby_nulls_first)?;

            if needs_projection {
                rel.inner = rel.inner.select(final_projection)?;
            }
        } else {
            let rel = self.relation_mut();
            rel.inner = rel.inner.select(projections)?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_aggregate_query(
        &mut self,
        projections: &Vec<Arc<Expr>>,
        schema: &Arc<Schema>,
        has_orderby: bool,
        groupby_exprs: Vec<Arc<Expr>>,
        query: &Query,
        projection_schema: &Schema,
        having: Option<Arc<Expr>>,
    ) -> Result<(), PlannerError> {
        let mut final_projection = Vec::with_capacity(projections.len());
        let mut aggs = Vec::with_capacity(projections.len());

        // these are orderbys that are part of the final projection
        let mut orderbys_after_projection = Vec::new();
        let mut orderbys_after_projection_desc = Vec::new();
        let mut orderbys_after_projection_nulls_first = Vec::new();

        // these are orderbys that are not part of the final projection
        let mut orderbys_before_projection = Vec::new();
        let mut orderbys_before_projection_desc = Vec::new();
        let mut orderbys_before_projection_nulls_first = Vec::new();

        for p in projections {
            let fld = p.to_field(schema)?;

            let name = fld.name.clone();
            if has_agg(p) {
                // this is an aggregate, so it is resolved during `.agg`. So we just push the column name
                final_projection.push(col(name.as_ref()));
                // add it to the aggs list
                aggs.push(p.clone());
            } else {
                // otherwise we just do a normal projection
                final_projection.push(p.clone());
            }
        }

        if let Some(having) = &having {
            if has_agg(having) {
                let having = having.alias(having.semantic_id(schema).id);

                aggs.push(having);
            }
        }

        let groupby_exprs = groupby_exprs
            .into_iter()
            .map(|e| {
                // instead of trying to do an additional projection for the groupby column, we just map it back to the original (unaliased) column
                // ex: SELECT a as b FROM t GROUP BY b
                // in this case, we need to resolve b to a
                if let Err(DaftError::FieldNotFound(_)) = e.to_field(schema) {
                    // this is likely an alias
                    unresolve_alias(e, &final_projection)
                } else {
                    Ok(e)
                }
            })
            .collect::<SQLPlannerResult<Vec<_>>>()?;

        if has_orderby {
            let order_by = query.order_by.as_ref().unwrap();

            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            };

            let (exprs, desc, nulls_first) = self.plan_order_by_exprs(order_by.exprs.as_slice())?;

            for (i, expr) in exprs.iter().enumerate() {
                // the orderby is ordered by a column of the projection
                // ex: SELECT a as b FROM t ORDER BY b
                // so we don't need an additional projection

                if let Ok(fld) = expr.to_field(projection_schema) {
                    // check if it's an aggregate
                    // ex: SELECT sum(a) FROM t ORDER BY sum(a)

                    // special handling for count(*)
                    // TODO: this is a hack, we should handle this better
                    //
                    // Since count(*) will always be `Ok` for `to_field(schema)`
                    // we need to manually check if it's in the final schema or not
                    if let Expr::Alias(e, alias) = expr.as_ref() {
                        if alias.as_ref() == "count"
                            && matches!(e.as_ref(), Expr::Agg(AggExpr::Count(_, CountMode::All)))
                        {
                            if let Some(alias) = aggs.iter().find_map(|agg| {
                                if let Expr::Alias(e, alias) = agg.as_ref() {
                                    if e == expr {
                                        Some(alias)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            }) {
                                // its a count(*) that is already in the final projection
                                // ex: SELECT count(*) as c FROM t ORDER BY count(*)
                                orderbys_after_projection.push(col(alias.as_ref()));
                                orderbys_after_projection_desc.push(desc[i]);
                                orderbys_after_projection_nulls_first.push(nulls_first[i]);
                            } else {
                                // its a count(*) that is not in the final projection
                                // ex: SELECT sum(n) FROM t ORDER BY count(*);
                                aggs.push(expr.clone());
                                orderbys_before_projection.push(col(fld.name.as_ref()));
                                orderbys_before_projection_desc.push(desc[i]);
                                orderbys_before_projection_nulls_first.push(nulls_first[i]);
                            }
                        }
                    } else if has_agg(expr) {
                        // aggregates part of the final projection are already resolved
                        // so we just need to push the column name
                        orderbys_after_projection.push(col(fld.name.as_ref()));
                        orderbys_after_projection_desc.push(desc[i]);
                        orderbys_after_projection_nulls_first.push(nulls_first[i]);
                    } else {
                        orderbys_after_projection.push(expr.clone());
                        orderbys_after_projection_desc.push(desc[i]);
                        orderbys_after_projection_nulls_first.push(nulls_first[i]);
                    }

                // the orderby is ordered by an expr from the original schema
                // ex: SELECT sum(b) FROM t ORDER BY sum(a)
                } else if let Ok(fld) = expr.to_field(schema) {
                    // check if it's an aggregate
                    if has_agg(expr) {
                        // check if it's an alias of something in the aggs
                        // if so, we can just use that column
                        // This way we avoid computing the aggregate twice
                        //
                        // ex: SELECT sum(a) as b FROM t ORDER BY sum(a);
                        if let Some(alias) = aggs.iter().find_map(|p| {
                            if let Expr::Alias(e, alias) = p.as_ref() {
                                if e == expr {
                                    Some(alias)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }) {
                            orderbys_after_projection.push(col(alias.as_ref()));
                            orderbys_after_projection_desc.push(desc[i]);
                            orderbys_after_projection_nulls_first.push(nulls_first[i]);
                        } else {
                            // its an aggregate that is not part of the final projection
                            // ex: SELECT sum(a) FROM t ORDER BY sum(b)
                            // so we need need to add it to the aggs list
                            aggs.push(expr.clone());

                            // then add it to the orderbys that are not part of the final projection
                            orderbys_before_projection.push(col(fld.name.as_ref()));
                            orderbys_before_projection_desc.push(desc[i]);
                            orderbys_before_projection_nulls_first.push(nulls_first[i]);
                        }
                    } else {
                        // we know it's a column of the original schema
                        // and its nt part of the final projection
                        // so we need an additional projection
                        // ex: SELECT sum(a) FROM t ORDER BY b

                        orderbys_before_projection.push(col(fld.name.as_ref()));
                        orderbys_before_projection_desc.push(desc[i]);
                        orderbys_before_projection_nulls_first.push(nulls_first[i]);
                    }
                } else {
                    panic!("unexpected order by expr");
                }
            }
        }

        let rel = self.relation_mut();
        rel.inner = rel.inner.aggregate(aggs.clone(), groupby_exprs)?;

        let has_orderby_before_projection = !orderbys_before_projection.is_empty();
        let has_orderby_after_projection = !orderbys_after_projection.is_empty();

        // ----------------
        // PERF(cory): if there are order bys from both parts, can we combine them into a single sort instead of two?
        // or can we optimize them into a single sort?
        // ----------------

        // order bys that are not in the final projection
        if has_orderby_before_projection {
            rel.inner = rel.inner.sort(
                orderbys_before_projection,
                orderbys_before_projection_desc,
                orderbys_before_projection_nulls_first,
            )?;
        }

        if let Some(having) = having {
            // if it's an agg, it's already resolved during .agg, so we just reference the column name
            let having = if has_agg(&having) {
                col(having.semantic_id(schema).id)
            } else {
                having
            };
            rel.inner = rel.inner.filter(having)?;
        }

        // apply the final projection
        rel.inner = rel.inner.select(final_projection)?;

        // order bys that are in the final projection
        if has_orderby_after_projection {
            rel.inner = rel.inner.sort(
                orderbys_after_projection,
                orderbys_after_projection_desc,
                orderbys_after_projection_nulls_first,
            )?;
        }

        Ok(())
    }

    fn plan_order_by_exprs(
        &self,
        expr: &[sqlparser::ast::OrderByExpr],
    ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<bool>, Vec<bool>)> {
        if expr.is_empty() {
            unsupported_sql_err!("ORDER BY []");
        }
        let mut exprs = Vec::with_capacity(expr.len());
        let mut desc = Vec::with_capacity(expr.len());
        let mut nulls_first = Vec::with_capacity(expr.len());
        for order_by_expr in expr {
            match (order_by_expr.asc, order_by_expr.nulls_first) {
                // ---------------------------
                // all of these are equivalent
                // ---------------------------
                // ORDER BY expr
                (None, None) |
                // ORDER BY expr ASC
                (Some(true), None) |
                // ORDER BY expr NULLS LAST
                (None, Some(false)) |
                // ORDER BY expr ASC NULLS LAST
                (Some(true), Some(false)) => {
                   nulls_first.push(false);
                   desc.push(false);
               },
                // ---------------------------


                // ---------------------------
                // ORDER BY expr NULLS FIRST
                (None, Some(true)) |
                // ORDER BY expr ASC NULLS FIRST
                (Some(true), Some(true)) => {
                    nulls_first.push(true);
                    desc.push(false);
                }
                // ---------------------------

                // ORDER BY expr DESC
                (Some(false), None) |
                // ORDER BY expr DESC NULLS FIRST
                (Some(false), Some(true)) => {
                    nulls_first.push(true);
                    desc.push(true);
                },
                // ORDER BY expr DESC NULLS LAST
                (Some(false), Some(false)) => {
                    nulls_first.push(false);
                    desc.push(true);
                }

            };
            if order_by_expr.with_fill.is_some() {
                unsupported_sql_err!("WITH FILL");
            }
            let expr = self.plan_expr(&order_by_expr.expr)?;

            exprs.push(expr);
        }
        Ok((exprs, desc, nulls_first))
    }

    fn plan_from(&mut self, from: &[TableWithJoins]) -> SQLPlannerResult<Relation> {
        if from.len() > 1 {
            let mut from_iter = from.iter();

            let first = from_iter.next().unwrap();
            let mut rel = self.new_with_context().plan_relation(&first.relation)?;
            self.table_map.insert(rel.get_name(), rel.clone());
            for tbl in from_iter {
                let right = self.new_with_context().plan_relation(&tbl.relation)?;
                self.table_map.insert(right.get_name(), right.clone());
                let right_join_prefix = Some(format!("{}.", right.get_name()));

                rel.inner =
                    rel.inner
                        .cross_join(right.inner, None, right_join_prefix.as_deref())?;
            }
            return Ok(rel);
        }

        let from = from.iter().next().unwrap();

        fn collect_idents(
            left: &[Ident],
            right: &[Ident],
            left_rel: &Relation,
            right_rel: &Relation,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>)> {
            let (left, right) = match (left, right) {
                // both are fully qualified: `join on a.x = b.y`
                ([tbl_a, Ident{value: col_a, ..}], [tbl_b, Ident{value: col_b, ..}]) => {
                    if left_rel.get_name() == tbl_b.value && right_rel.get_name() == tbl_a.value {
                        (col_b.clone(), col_a.clone())
                    } else {
                        (col_a.clone(), col_b.clone())
                    }
                }
                // only one is fully qualified: `join on x = b.y`
                ([Ident{value: col_a, ..}], [tbl_b, Ident{value: col_b, ..}]) => {
                    if tbl_b.value == right_rel.get_name() {
                        (col_a.clone(), col_b.clone())
                    } else if tbl_b.value == left_rel.get_name() {
                        (col_b.clone(), col_a.clone())
                    } else {
                        unsupported_sql_err!("Could not determine which table the identifiers belong to")
                    }
                }
                // only one is fully qualified: `join on a.x = y`
                ([tbl_a, Ident{value: col_a, ..}], [Ident{value: col_b, ..}]) => {
                    // find out which one the qualified identifier belongs to
                    // we assume the other identifier belongs to the other table
                    if tbl_a.value == left_rel.get_name() {
                        (col_a.clone(), col_b.clone())
                    } else if tbl_a.value == right_rel.get_name() {
                        (col_b.clone(), col_a.clone())
                    } else {
                        unsupported_sql_err!("Could not determine which table the identifiers belong to")
                    }
                }
                // neither are fully qualified: `join on x = y`
                ([left], [right]) => {
                    let left = ident_to_str(left);
                    let right = ident_to_str(right);

                    // we don't know which table the identifiers belong to, so we need to check both
                    let left_schema = left_rel.schema();
                    let right_schema = right_rel.schema();

                    // if the left side is in the left schema, then we assume the right side is in the right schema
                    if left_schema.get_field(&left).is_ok() {
                        (left, right)
                    // if the right side is in the left schema, then we assume the left side is in the right schema
                    } else if right_schema.get_field(&left).is_ok() {
                        (right, left)
                    } else {
                        unsupported_sql_err!("JOIN clauses must reference columns in the joined tables; found `{}`", left);
                    }

                }
                _ => unsupported_sql_err!(
                    "collect_compound_identifiers: Expected left.len() == 2 && right.len() == 2, but found left.len() == {:?}, right.len() == {:?}",
                    left.len(),
                    right.len()
                ),
            };
            Ok((vec![col(left)], vec![col(right)]))
        }

        fn process_join_on(
            expression: &sqlparser::ast::Expr,
            left_rel: &Relation,
            right_rel: &Relation,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>, Vec<bool>)> {
            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expression {
                match *op {
                    BinaryOperator::Eq | BinaryOperator::Spaceship => {
                        let null_equals_null = *op == BinaryOperator::Spaceship;

                        let left = get_idents_vec(left)?;
                        let right = get_idents_vec(right)?;

                        collect_idents(&left, &right, left_rel, right_rel)
                            .map(|(left, right)| (left, right, vec![null_equals_null]))
                    }
                    BinaryOperator::And => {
                        let (mut left_i, mut right_i, mut null_equals_nulls_i) =
                            process_join_on(left, left_rel, right_rel)?;
                        let (mut left_j, mut right_j, mut null_equals_nulls_j) =
                            process_join_on(right, left_rel, right_rel)?;
                        left_i.append(&mut left_j);
                        right_i.append(&mut right_j);
                        null_equals_nulls_i.append(&mut null_equals_nulls_j);
                        Ok((left_i, right_i, null_equals_nulls_i))
                    }
                    _ => {
                        unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found op = '{}'", op);
                    }
                }
            } else if let sqlparser::ast::Expr::Nested(expr) = expression {
                process_join_on(expr, left_rel, right_rel)
            } else {
                unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found expression = {:?}", expression);
            }
        }

        let relation = from.relation.clone();
        let mut left_rel = self.new_with_context().plan_relation(&relation)?;
        self.table_map.insert(left_rel.get_name(), left_rel.clone());

        for join in &from.joins {
            use sqlparser::ast::{
                JoinConstraint,
                JoinOperator::{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter},
            };
            let right_rel = self.new_with_context().plan_relation(&join.relation)?;
            self.table_map
                .insert(right_rel.get_name(), right_rel.clone());
            let right_rel_name = right_rel.get_name();
            let right_join_prefix = Some(format!("{right_rel_name}."));

            let (join_type, constraint) = match &join.join_operator {
                Inner(constraint) => (JoinType::Inner, constraint),
                LeftOuter(constraint) => (JoinType::Left, constraint),
                RightOuter(constraint) => (JoinType::Right, constraint),
                FullOuter(constraint) => (JoinType::Outer, constraint),
                LeftSemi(constraint) => (JoinType::Semi, constraint),
                LeftAnti(constraint) => (JoinType::Anti, constraint),

                _ => unsupported_sql_err!("Unsupported join type: {:?}", join.join_operator),
            };

            let (left_on, right_on, null_eq_null, keep_join_keys) = match &constraint {
                JoinConstraint::On(expr) => {
                    let (left_on, right_on, null_equals_nulls) =
                        process_join_on(expr, &left_rel, &right_rel)?;
                    (left_on, right_on, Some(null_equals_nulls), true)
                }
                JoinConstraint::Using(idents) => {
                    let on = idents
                        .iter()
                        .map(|i| col(i.value.clone()))
                        .collect::<Vec<_>>();
                    (on.clone(), on, None, false)
                }
                JoinConstraint::Natural => unsupported_sql_err!("NATURAL JOIN not supported"),
                JoinConstraint::None => unsupported_sql_err!("JOIN without ON/USING not supported"),
            };

            left_rel.inner = left_rel.inner.join_with_null_safe_equal(
                right_rel.inner,
                left_on,
                right_on,
                null_eq_null,
                join_type,
                None,
                None,
                right_join_prefix.as_deref(),
                keep_join_keys,
            )?;
        }

        Ok(left_rel)
    }

    fn plan_relation(&self, rel: &sqlparser::ast::TableFactor) -> SQLPlannerResult<Relation> {
        let (rel, alias) = match rel {
            sqlparser::ast::TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } => {
                let tbl_fn = name.0.first().unwrap().value.as_str();
                (self.plan_table_function(tbl_fn, args)?, alias.clone())
            }
            sqlparser::ast::TableFactor::Table {
                name,
                args: None,
                alias,
                ..
            } => {
                let table_name = name.to_string();
                let Some(rel) = self
                    .table_map
                    .get(&table_name)
                    .cloned()
                    .or_else(|| self.cte_map().get(&table_name).cloned())
                    .or_else(|| {
                        self.catalog()
                            .get_table(&table_name)
                            .map(|table| Relation::new(table.into(), table_name.clone()))
                    })
                else {
                    table_not_found_err!(table_name)
                };

                (rel, alias.clone())
            }
            sqlparser::ast::TableFactor::Derived {
                lateral,
                subquery,
                alias: Some(alias),
            } => {
                if *lateral {
                    unsupported_sql_err!("LATERAL");
                }
                let subquery = self.new_with_context().plan_query(subquery)?;
                let rel_name = ident_to_str(&alias.name);
                let rel = Relation::new(subquery, rel_name);

                (rel, Some(alias.clone()))
            }
            sqlparser::ast::TableFactor::TableFunction { .. } => {
                unsupported_sql_err!("Unsupported table factor: TableFunction")
            }
            sqlparser::ast::TableFactor::Function { .. } => {
                unsupported_sql_err!("Unsupported table factor: Function")
            }
            sqlparser::ast::TableFactor::UNNEST { .. } => {
                unsupported_sql_err!("Unsupported table factor: UNNEST")
            }
            sqlparser::ast::TableFactor::JsonTable { .. } => {
                unsupported_sql_err!("Unsupported table factor: JsonTable")
            }
            sqlparser::ast::TableFactor::NestedJoin { .. } => {
                unsupported_sql_err!("Unsupported table factor: NestedJoin")
            }
            sqlparser::ast::TableFactor::Pivot { .. } => {
                unsupported_sql_err!("Unsupported table factor: Pivot")
            }
            sqlparser::ast::TableFactor::Unpivot { .. } => {
                unsupported_sql_err!("Unsupported table factor: Unpivot")
            }
            sqlparser::ast::TableFactor::MatchRecognize { .. } => {
                unsupported_sql_err!("Unsupported table factor: MatchRecognize")
            }
            _ => unsupported_sql_err!("Unsupported table factor"),
        };
        if let Some(alias) = alias {
            Ok(rel.with_alias(alias))
        } else {
            Ok(rel)
        }
    }

    fn plan_identifier(&self, idents: &[Ident]) -> SQLPlannerResult<ExprRef> {
        // if the current relation is not resolved (e.g. in a `sql_expr` call, simply wrap identifier in a col)
        if self.current_relation.is_none() {
            return Ok(col(idents_to_str(idents)));
        }

        /// Helper function that produces either a column or outer reference column
        /// depending on the depth
        fn maybe_outer_col(field: Field, depth: u64) -> ExprRef {
            if depth == 0 {
                Arc::new(Expr::Column(field.name.into()))
            } else {
                Arc::new(Expr::OuterReferenceColumn(OuterReferenceColumn {
                    field,
                    depth,
                }))
            }
        }

        let full_str = idents_to_str(idents);

        let [root, rest @ ..] = idents else {
            return Err(PlannerError::ParseError {
                message: "empty identifier".to_string(),
            });
        };

        let current_relation_name = self.relation_opt().unwrap().get_name();

        let root_str = ident_to_str(root);
        let rest_str = idents_to_str(rest);

        let mut depth = 0;

        let mut curr_planner = Some(self);

        // loop through parent plans until we find the identifier in the schema
        while let Some(planner) = curr_planner {
            let plan_relation = planner.relation_opt().unwrap();

            // Try to find in schema at current depth
            // This also covers duplicate columns from joins, which are added to the table with a prefix (df.column_name)
            if let Ok(field) = plan_relation.schema().get_field(&full_str) {
                return Ok(maybe_outer_col(field.clone(), depth));
            }
            // The identifier could also be in the alias map but not the schema
            // for expressions in WHERE, GROUP BY, and HAVING, which are done before project
            if let Some(expr) = planner.alias_map.get(&full_str) {
                // transform expression alias map by incrementing thee depths of every column by `depth`
                let transformed_expr = expr
                    .clone()
                    .transform(|e| match e.as_ref() {
                        Expr::Column(name) => {
                            let field = plan_relation.schema().get_field(name)?.clone();
                            Ok(Transformed::yes(maybe_outer_col(field, depth)))
                        }
                        Expr::OuterReferenceColumn(c) => Ok(Transformed::yes(maybe_outer_col(
                            c.field.clone(),
                            c.depth + depth,
                        ))),
                        _ => Ok(Transformed::no(e)),
                    })?
                    .data;

                return Ok(transformed_expr);
            }

            // If compound identifier, try to find in tables at current depth
            if !rest.is_empty()
                && let Some(relation) = planner.table_map.get(&root_str)
            {
                let relation_schema = relation.schema();

                if let Ok(field) = relation_schema.get_field(&rest_str) {
                    return Ok(maybe_outer_col(field.clone(), depth));
                } else {
                    column_not_found_err!(&full_str, current_relation_name)
                }
            }

            curr_planner = planner.parent;
            depth += 1;
        }

        if rest.is_empty() {
            column_not_found_err!(&full_str, current_relation_name)
        } else {
            table_not_found_err!(root_str)
        }
    }

    fn select_item_to_expr(
        &mut self,
        item: &SelectItem,
        schema: &Schema,
    ) -> SQLPlannerResult<Vec<ExprRef>> {
        fn wildcard_exclude(
            schema: SchemaRef,
            exclusion: &ExcludeSelectItem,
        ) -> DaftResult<Schema> {
            match exclusion {
                ExcludeSelectItem::Single(column) => schema.exclude(&[&column.to_string()]),
                ExcludeSelectItem::Multiple(columns) => {
                    let items = columns
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<String>>();

                    schema.exclude(items.as_slice())
                }
            }
        }

        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.plan_expr(expr)?;
                let alias = alias.value.to_string();
                self.alias_map.insert(alias.clone(), expr.clone());
                Ok(vec![expr.alias(alias)])
            }
            SelectItem::UnnamedExpr(expr) => self.plan_expr(expr).map(|e| vec![e]),

            SelectItem::Wildcard(wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;

                if let Some(exclude) = &wildcard_opts.opt_exclude {
                    let current_relation = match &self.current_relation {
                        Some(rel) => rel,
                        None => {
                            return Err(PlannerError::TableNotFound {
                                message: "No table found to exclude columns from".to_string(),
                            })
                        }
                    };
                    let schema = current_relation.inner.schema();
                    wildcard_exclude(schema, exclude)
                        .map(|schema| {
                            schema
                                .names()
                                .iter()
                                .map(|n| col(n.as_ref()))
                                .collect::<Vec<_>>()
                        })
                        .map_err(std::convert::Into::into)
                } else if schema.is_empty() {
                    Ok(vec![col("*")])
                } else {
                    Ok(schema
                        .names()
                        .iter()
                        .map(|n| col(n.as_ref()))
                        .collect::<Vec<_>>())
                }
            }
            SelectItem::QualifiedWildcard(object_name, wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;
                let table_name = idents_to_str(&object_name.0);
                let Some(rel) = self.relation_opt() else {
                    table_not_found_err!(table_name);
                };
                let Some(table_rel) = self.table_map.get(&table_name) else {
                    table_not_found_err!(table_name);
                };
                let right_schema = table_rel.inner.schema();
                let keys = schema.fields.keys();

                let right_schema = if let Some(exclude) = &wildcard_opts.opt_exclude {
                    Arc::new(wildcard_exclude(right_schema, exclude)?)
                } else {
                    right_schema
                };

                let columns = right_schema
                    .fields
                    .keys()
                    .map(|field| {
                        if keys
                            .clone()
                            .any(|s| s.starts_with(&table_name) && s.ends_with(field))
                        {
                            if table_name == rel.get_name() {
                                col(field.clone())
                            } else {
                                col(format!("{}.{}", &table_name, field)).alias(field.as_ref())
                            }
                        } else {
                            col(field.clone())
                        }
                    })
                    .collect::<Vec<_>>();

                Ok(columns)
            }
        }
    }

    fn value_to_lit(&self, value: &Value) -> SQLPlannerResult<LiteralValue> {
        Ok(match value {
            Value::SingleQuotedString(s) => LiteralValue::Utf8(s.clone()),
            Value::Number(n, _) => n
                .parse::<i64>()
                .map(LiteralValue::Int64)
                .or_else(|_| n.parse::<f64>().map(LiteralValue::Float64))
                .map_err(|_| {
                    PlannerError::invalid_operation(format!(
                        "could not parse number literal '{n:?}'"
                    ))
                })?,
            Value::Boolean(b) => LiteralValue::Boolean(*b),
            Value::Null => LiteralValue::Null,
            _ => {
                return Err(PlannerError::invalid_operation(
                    "Only string, number, boolean and null literals are supported. Instead found: `{value}`",
                ))
            }
        })
    }

    pub(crate) fn plan_expr(&self, expr: &sqlparser::ast::Expr) -> SQLPlannerResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => self.plan_identifier(std::slice::from_ref(ident)),
            SQLExpr::Value(v) => self.value_to_lit(v).map(Expr::Literal).map(Arc::new),
            SQLExpr::BinaryOp { left, op, right } => {
                let left = self.plan_expr(left)?;
                let right = self.plan_expr(right)?;
                let op = self.sql_operator_to_operator(op)?;
                Ok(Expr::BinaryOp { left, op, right }.arced())
            }
            SQLExpr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                expr,
                data_type,
                format: None,
            } => {
                let dtype = self.sql_dtype_to_dtype(data_type)?;
                let expr = self.plan_expr(expr)?;
                Ok(expr.cast(&dtype))
            }
            SQLExpr::IsFalse(expr) => Ok(self.plan_expr(expr)?.eq(lit(false))),
            SQLExpr::IsNotFalse(_) => Ok(self.plan_expr(expr)?.eq(lit(false)).not()),
            SQLExpr::IsTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true))),
            SQLExpr::IsNotTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true)).not()),
            SQLExpr::IsNull(expr) => Ok(self.plan_expr(expr)?.is_null()),
            SQLExpr::IsNotNull(expr) => Ok(self.plan_expr(expr)?.is_null().not()),
            SQLExpr::UnaryOp { op, expr } => self.plan_unary_op(op, expr),
            SQLExpr::CompoundIdentifier(idents) => self.plan_identifier(idents),
            SQLExpr::CompositeAccess { .. } => {
                unsupported_sql_err!("composite access")
            }
            SQLExpr::IsUnknown(_) => unsupported_sql_err!("IS UNKNOWN"),
            SQLExpr::IsNotUnknown(_) => {
                unsupported_sql_err!("IS NOT UNKNOWN")
            }
            SQLExpr::IsDistinctFrom(_, _) => {
                unsupported_sql_err!("IS DISTINCT FROM")
            }
            SQLExpr::IsNotDistinctFrom(_, _) => {
                unsupported_sql_err!("IS NOT DISTINCT FROM")
            }
            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.plan_expr(expr)?;
                let list = list
                    .iter()
                    .map(|e| self.plan_expr(e))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;

                let expr = expr.is_in(list);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.plan_expr(expr)?;
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?.build();
                let subquery = Subquery { plan: subquery };

                if *negated {
                    Ok(expr.in_subquery(subquery).not())
                } else {
                    Ok(expr.in_subquery(subquery))
                }
            }
            SQLExpr::InUnnest { .. } => unsupported_sql_err!("IN UNNEST"),
            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.plan_expr(expr)?;
                let low = self.plan_expr(low)?;
                let high = self.plan_expr(high)?;
                let expr = expr.between(low, high);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                if escape_char.is_some() {
                    unsupported_sql_err!("LIKE with escape char")
                }
                let expr = self.plan_expr(expr)?;
                let pattern = self.plan_expr(pattern)?;
                let expr = like(expr, pattern);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                if escape_char.is_some() {
                    unsupported_sql_err!("ILIKE with escape char")
                }
                let expr = self.plan_expr(expr)?;
                let pattern = self.plan_expr(pattern)?;
                let expr = ilike(expr, pattern);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::SimilarTo { .. } => unsupported_sql_err!("SIMILAR TO"),
            SQLExpr::RLike { .. } => unsupported_sql_err!("RLIKE"),
            SQLExpr::AnyOp { .. } => unsupported_sql_err!("ANY"),
            SQLExpr::AllOp { .. } => unsupported_sql_err!("ALL"),
            SQLExpr::Convert { .. } => unsupported_sql_err!("CONVERT"),
            SQLExpr::Cast { .. } => unsupported_sql_err!("CAST"),
            SQLExpr::AtTimeZone { .. } => unsupported_sql_err!("AT TIME ZONE"),
            SQLExpr::Extract {
                field,
                syntax: _,
                expr,
            } => {
                use daft_functions::temporal::{self as dt};
                let expr = self.plan_expr(expr)?;

                match field {
                    DateTimeField::Year => Ok(dt::dt_year(expr)),
                    DateTimeField::Month => Ok(dt::dt_month(expr)),
                    DateTimeField::Day => Ok(dt::dt_day(expr)),
                    DateTimeField::DayOfWeek => Ok(dt::dt_day_of_week(expr)),
                    DateTimeField::Date => Ok(dt::dt_date(expr)),
                    DateTimeField::Hour => Ok(dt::dt_hour(expr)),
                    DateTimeField::Minute => Ok(dt::dt_minute(expr)),
                    DateTimeField::Second => Ok(dt::dt_second(expr)),
                    other => unsupported_sql_err!("EXTRACT ({other})"),
                }
            }
            SQLExpr::Ceil { expr, .. } => Ok(ceil(self.plan_expr(expr)?)),
            SQLExpr::Floor { expr, .. } => Ok(floor(self.plan_expr(expr)?)),
            SQLExpr::Position { .. } => unsupported_sql_err!("POSITION"),
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: true, // We only support SUBSTRING(expr, start, length) syntax
            } => {
                let (Some(substring_from), Some(substring_for)) = (substring_from, substring_for)
                else {
                    unsupported_sql_err!("SUBSTRING")
                };

                let expr = self.plan_expr(expr)?;
                let start = self.plan_expr(substring_from)?;
                let length = self.plan_expr(substring_for)?;

                Ok(daft_functions::utf8::substr(expr, start, length))
            }
            SQLExpr::Substring { special: false, .. } => {
                unsupported_sql_err!("`SUBSTRING(expr [FROM start] [FOR len])` syntax")
            }
            SQLExpr::Trim { .. } => unsupported_sql_err!("TRIM"),
            SQLExpr::Overlay { .. } => unsupported_sql_err!("OVERLAY"),
            SQLExpr::Collate { .. } => unsupported_sql_err!("COLLATE"),
            SQLExpr::Nested(e) => self.plan_expr(e),
            SQLExpr::IntroducedString { .. } => unsupported_sql_err!("INTRODUCED STRING"),
            SQLExpr::TypedString { data_type, value } => match data_type {
                sqlparser::ast::DataType::Date => Ok(to_date(lit(value.as_str()), "%Y-%m-%d")),
                sqlparser::ast::DataType::Timestamp(None, TimezoneInfo::None)
                | sqlparser::ast::DataType::Datetime(None) => Ok(to_datetime(
                    lit(value.as_str()),
                    "%Y-%m-%d %H:%M:%S %z",
                    None,
                )),
                dtype => {
                    unsupported_sql_err!("TypedString with data type {:?}", dtype)
                }
            },
            SQLExpr::Function(func) => self.plan_function(func),
            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if operand.is_some() {
                    unsupported_sql_err!("CASE with operand not yet supported");
                }
                if results.len() != conditions.len() {
                    unsupported_sql_err!("CASE with different number of conditions and results");
                }

                let else_expr = match else_result {
                    Some(expr) => self.plan_expr(expr)?,
                    None => unsupported_sql_err!("CASE with no else result"),
                };

                // we need to traverse from back to front to build the if else chain
                // because we need to start with the else expression
                conditions.iter().zip(results.iter()).rev().try_fold(
                    else_expr,
                    |else_expr, (condition, result)| {
                        let cond = self.plan_expr(condition)?;
                        let res = self.plan_expr(result)?;
                        Ok(cond.if_else(res, else_expr))
                    },
                )
            }
            SQLExpr::Exists { subquery, negated } => {
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?;
                let subquery = Subquery {
                    plan: subquery.build(),
                };
                if *negated {
                    Ok(Expr::Exists(subquery).arced().not())
                } else {
                    Ok(Expr::Exists(subquery).arced())
                }
            }
            SQLExpr::Subquery(subquery) => {
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?;
                let subquery = Subquery {
                    plan: subquery.build(),
                };
                Ok(Expr::Subquery(subquery).arced())
            }
            SQLExpr::GroupingSets(_) => unsupported_sql_err!("GROUPING SETS"),
            SQLExpr::Cube(_) => unsupported_sql_err!("CUBE"),
            SQLExpr::Rollup(_) => unsupported_sql_err!("ROLLUP"),
            // Similar to rust and python conventions, tuples always have a fixed size,
            // so we convert them to a fixed size list.
            SQLExpr::Tuple(exprs) => {
                let values = exprs
                    .iter()
                    .map(|expr| {
                        let expr = self.plan_expr(expr)?;
                        // this should always unwrap
                        // there is no way to have multiple references to the same expr at this point
                        let expr = Arc::unwrap_or_clone(expr);
                        match expr {
                            Expr::Literal(lit) => Ok(lit),
                            _ => unsupported_sql_err!("Tuple with non-literal"),
                        }
                    })
                    .collect::<SQLPlannerResult<Vec<_>>>()?;

                let s = literals_to_series(&values)?;
                let s = FixedSizeListArray::new(
                    Field::new("tuple", s.data_type().clone())
                        .to_fixed_size_list_field(exprs.len())?,
                    s,
                    None,
                )
                .into_series();

                Ok(lit(s))
            }
            SQLExpr::Struct { .. } => unsupported_sql_err!("STRUCT"),
            SQLExpr::Named { .. } => unsupported_sql_err!("NAMED"),
            SQLExpr::Dictionary(dict) => {
                let entries = dict
                    .iter()
                    .map(|entry| {
                        let key = entry.key.value.clone();
                        let value = self.plan_expr(&entry.value)?;
                        let value = value.as_literal().ok_or_else(|| {
                            PlannerError::invalid_operation("Dictionary value is not a literal")
                        })?;
                        let struct_field = Field::new(key, value.get_type());
                        Ok((struct_field, value.clone()))
                    })
                    .collect::<SQLPlannerResult<_>>()?;

                Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
            }
            SQLExpr::Map(_) => unsupported_sql_err!("MAP"),
            SQLExpr::Subscript { expr, subscript } => self.plan_subscript(expr, subscript.as_ref()),
            SQLExpr::Array(_) => unsupported_sql_err!("ARRAY"),
            SQLExpr::Interval(interval) => {
                use regex::Regex;

                /// A private struct represents a single parsed interval unit and its value
                #[derive(Debug)]
                struct IntervalPart {
                    count: i64,
                    unit: DateTimeField,
                }

                // Local function to parse interval string to interval parts
                fn parse_interval_string(expr: &str) -> Result<Vec<IntervalPart>, PlannerError> {
                    let expr = expr.trim().trim_matches('\'');

                    let re = Regex::new(r"(-?\d+)\s*(year|years|month|months|day|days|hour|hours|minute|minutes|second|seconds|millisecond|milliseconds|microsecond|microseconds|nanosecond|nanoseconds|week|weeks)")
                        .map_err(|e|PlannerError::invalid_operation(format!("Invalid regex pattern: {}", e)))?;

                    let mut parts = Vec::new();

                    for cap in re.captures_iter(expr) {
                        let count: i64 = cap[1].parse().map_err(|e| {
                            PlannerError::invalid_operation(format!("Invalid interval count: {e}"))
                        })?;

                        let unit = match &cap[2].to_lowercase()[..] {
                            "year" | "years" => DateTimeField::Year,
                            "month" | "months" => DateTimeField::Month,
                            "week" | "weeks" => DateTimeField::Week(None),
                            "day" | "days" => DateTimeField::Day,
                            "hour" | "hours" => DateTimeField::Hour,
                            "minute" | "minutes" => DateTimeField::Minute,
                            "second" | "seconds" => DateTimeField::Second,
                            "millisecond" | "milliseconds" => DateTimeField::Millisecond,
                            "microsecond" | "microseconds" => DateTimeField::Microsecond,
                            "nanosecond" | "nanoseconds" => DateTimeField::Nanosecond,
                            _ => {
                                return Err(PlannerError::invalid_operation(format!(
                                    "Invalid interval unit: {}",
                                    &cap[2]
                                )))
                            }
                        };

                        parts.push(IntervalPart { count, unit });
                    }

                    if parts.is_empty() {
                        return Err(PlannerError::invalid_operation("Invalid interval format."));
                    }

                    Ok(parts)
                }

                // Local function to convert parts to interval values
                fn interval_parts_to_values(parts: Vec<IntervalPart>) -> (i64, i64, i64) {
                    let mut total_months = 0i64;
                    let mut total_days = 0i64;
                    let mut total_nanos = 0i64;

                    for part in parts {
                        match part.unit {
                            DateTimeField::Year => total_months += 12 * part.count,
                            DateTimeField::Month => total_months += part.count,
                            DateTimeField::Week(_) => total_days += 7 * part.count,
                            DateTimeField::Day => total_days += part.count,
                            DateTimeField::Hour => total_nanos += part.count * 3_600_000_000_000,
                            DateTimeField::Minute => total_nanos += part.count * 60_000_000_000,
                            DateTimeField::Second => total_nanos += part.count * 1_000_000_000,
                            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                                total_nanos += part.count * 1_000_000;
                            }
                            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                                total_nanos += part.count * 1_000;
                            }
                            DateTimeField::Nanosecond | DateTimeField::Nanoseconds => {
                                total_nanos += part.count;
                            }
                            _ => {}
                        }
                    }

                    (total_months, total_days, total_nanos)
                }

                match interval {
                    // If leading_field is specified, treat it as the old style single-unit interval
                    // e.g., INTERVAL '12' YEAR
                    sqlparser::ast::Interval {
                        value,
                        leading_field: Some(time_unit),
                        ..
                    } => {
                        let expr = self.plan_expr(value)?;

                        let expr =
                            expr.as_literal()
                                .and_then(|lit| lit.as_str())
                                .ok_or_else(|| {
                                    PlannerError::invalid_operation(
                                        "Interval value must be a string",
                                    )
                                })?;

                        let count = expr.parse::<i64>().map_err(|e| {
                            PlannerError::unsupported_sql(format!("Invalid interval count: {e}"))
                        })?;

                        let (months, days, nanoseconds) = match time_unit {
                            DateTimeField::Year => (12 * count, 0, 0),
                            DateTimeField::Month => (count, 0, 0),
                            DateTimeField::Week(_) => (0, 7 * count, 0),
                            DateTimeField::Day => (0, count, 0),
                            DateTimeField::Hour => (0, 0, count * 3_600_000_000_000),
                            DateTimeField::Minute => (0, 0, count * 60_000_000_000),
                            DateTimeField::Second => (0, 0, count * 1_000_000_000),
                            DateTimeField::Microsecond | DateTimeField::Microseconds => (0, 0, count * 1_000),
                            DateTimeField::Millisecond | DateTimeField::Milliseconds => (0, 0, count * 1_000_000),
                            DateTimeField::Nanosecond | DateTimeField::Nanoseconds => (0, 0, count),
                            _ => return Err(PlannerError::invalid_operation(format!(
                                "Invalid interval unit: {time_unit}. Expected one of: year, month, week, day, hour, minute, second, millisecond, microsecond, nanosecond"
                            ))),
                        };

                        Ok(Arc::new(Expr::Literal(LiteralValue::Interval(
                            daft_core::datatypes::IntervalValue::new(
                                months as i32,
                                days as i32,
                                nanoseconds,
                            ),
                        ))))
                    }

                    // If no leading_field is specified, treat it as the new style multi-unit interval
                    // e.g., INTERVAL '12 years 3 months 7 days'
                    sqlparser::ast::Interval {
                        value,
                        leading_field: None,
                        ..
                    } => {
                        let expr = self.plan_expr(value)?;

                        let expr =
                            expr.as_literal()
                                .and_then(|lit| lit.as_str())
                                .ok_or_else(|| {
                                    PlannerError::invalid_operation(
                                        "Interval value must be a string",
                                    )
                                })?;

                        let parts = parse_interval_string(expr)
                            .map_err(|e| PlannerError::invalid_operation(e.to_string()))?;

                        let (months, days, nanoseconds) = interval_parts_to_values(parts);

                        Ok(Arc::new(Expr::Literal(LiteralValue::Interval(
                            daft_core::datatypes::IntervalValue::new(
                                months as i32,
                                days as i32,
                                nanoseconds,
                            ),
                        ))))
                    }
                }
            }
            SQLExpr::MatchAgainst { .. } => unsupported_sql_err!("MATCH AGAINST"),
            SQLExpr::Wildcard => unsupported_sql_err!("WILDCARD"),
            SQLExpr::QualifiedWildcard(_) => unsupported_sql_err!("QUALIFIED WILDCARD"),
            SQLExpr::OuterJoin(_) => unsupported_sql_err!("OUTER JOIN"),
            SQLExpr::Prior(_) => unsupported_sql_err!("PRIOR"),
            SQLExpr::Lambda(_) => unsupported_sql_err!("LAMBDA"),
            SQLExpr::JsonAccess { .. } | SQLExpr::MapAccess { .. } => {
                unreachable!("Not reachable in our dialect, should always be parsed as subscript")
            }
        }
    }

    fn sql_operator_to_operator(&self, op: &BinaryOperator) -> SQLPlannerResult<Operator> {
        match op {
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::TrueDivide),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::Modulo => Ok(Operator::Modulus),
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::DuckIntegerDivide => Ok(Operator::FloorDivide),
            other => unsupported_sql_err!("Unsupported operator: '{other}'"),
        }
    }
    fn sql_dtype_to_dtype(&self, dtype: &sqlparser::ast::DataType) -> SQLPlannerResult<DataType> {
        use sqlparser::ast::DataType as SQLDataType;

        Ok(match dtype {
            // ---------------------------------
            // array/list
            // ---------------------------------
            SQLDataType::Array(
                ArrayElemTypeDef::AngleBracket(inner_type)
                | ArrayElemTypeDef::SquareBracket(inner_type, None),
            ) => DataType::List(Box::new(self.sql_dtype_to_dtype(inner_type)?)),
            SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_type, Some(size))) => {
                DataType::FixedSizeList(
                    Box::new(self.sql_dtype_to_dtype(inner_type)?),
                    *size as usize,
                )
            }

            // ---------------------------------
            // binary
            // ---------------------------------
            SQLDataType::Bytea
            | SQLDataType::Bytes(_)
            | SQLDataType::Binary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Varbinary(_) => DataType::Binary,

            // ---------------------------------
            // boolean
            // ---------------------------------
            SQLDataType::Boolean | SQLDataType::Bool => DataType::Boolean,
            // ---------------------------------
            // signed integer
            // ---------------------------------
            SQLDataType::Int(_) | SQLDataType::Integer(_) => DataType::Int32,
            SQLDataType::Int2(_) | SQLDataType::SmallInt(_) => DataType::Int16,
            SQLDataType::Int4(_) | SQLDataType::MediumInt(_) => DataType::Int32,
            SQLDataType::Int8(_) | SQLDataType::BigInt(_) => DataType::Int64,
            SQLDataType::TinyInt(_) => DataType::Int8,
            // ---------------------------------
            // unsigned integer
            // ---------------------------------
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => DataType::UInt32,
            SQLDataType::UnsignedInt2(_) | SQLDataType::UnsignedSmallInt(_) => DataType::UInt16,
            SQLDataType::UnsignedInt4(_) | SQLDataType::UnsignedMediumInt(_) => DataType::UInt32,
            SQLDataType::UnsignedInt8(_) | SQLDataType::UnsignedBigInt(_) => DataType::UInt64,
            SQLDataType::UnsignedTinyInt(_) => DataType::UInt8,
            // ---------------------------------
            // float
            // ---------------------------------
            SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => {
                DataType::Float64
            }
            SQLDataType::Float(n_bytes) => match n_bytes {
                Some(n) if (1u64..=24u64).contains(n) => DataType::Float32,
                Some(n) if (25u64..=53u64).contains(n) => DataType::Float64,
                Some(n) => {
                    unsupported_sql_err!(
                        "unsupported `float` size (expected a value between 1 and 53, found {})",
                        n
                    )
                }
                None => DataType::Float64,
            },
            SQLDataType::Float4 | SQLDataType::Real => DataType::Float32,

            // ---------------------------------
            // decimal
            // ---------------------------------
            SQLDataType::Dec(info) | SQLDataType::Decimal(info) | SQLDataType::Numeric(info) => {
                match *info {
                    ExactNumberInfo::PrecisionAndScale(p, s) => {
                        DataType::Decimal128(p as usize, s as usize)
                    }
                    ExactNumberInfo::Precision(p) => DataType::Decimal128(p as usize, 0),
                    ExactNumberInfo::None => DataType::Decimal128(38, 9),
                }
            }
            // ---------------------------------
            // temporal
            // ---------------------------------
            SQLDataType::Date => DataType::Date,
            SQLDataType::Interval => DataType::Duration(TimeUnit::Microseconds),
            SQLDataType::Time(precision, tz) => match tz {
                TimezoneInfo::None => DataType::Time(self.timeunit_from_precision(precision)?),
                _ => unsupported_sql_err!("`time` with timezone is; found tz={}", tz),
            },
            SQLDataType::Datetime(_) => unsupported_sql_err!("`datetime` is not supported"),
            SQLDataType::Timestamp(prec, tz) => match tz {
                TimezoneInfo::None => {
                    DataType::Timestamp(self.timeunit_from_precision(prec)?, None)
                }
                _ => unsupported_sql_err!("`timestamp` with timezone"),
            },
            // ---------------------------------
            // string
            // ---------------------------------
            SQLDataType::Char(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::Clob(_)
            | SQLDataType::String(_)
            | SQLDataType::Text
            | SQLDataType::Uuid
            | SQLDataType::Varchar(_) => DataType::Utf8,
            // ---------------------------------
            // struct
            // ---------------------------------
            SQLDataType::Struct(fields, _) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(
                        |(
                            idx,
                            StructField {
                                field_name,
                                field_type,
                            },
                        )| {
                            let dtype = self.sql_dtype_to_dtype(field_type)?;
                            let name = match field_name {
                                Some(name) => name.to_string(),
                                None => format!("col_{idx}"),
                            };

                            Ok(Field::new(name, dtype))
                        },
                    )
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
                DataType::Struct(fields)
            }
            other => unsupported_sql_err!("data type: {:?}", other),
        })
    }

    fn timeunit_from_precision(&self, prec: &Option<u64>) -> SQLPlannerResult<TimeUnit> {
        Ok(match prec {
            None => TimeUnit::Microseconds,
            Some(n) if (1u64..=3u64).contains(n) => TimeUnit::Milliseconds,
            Some(n) if (4u64..=6u64).contains(n) => TimeUnit::Microseconds,
            Some(n) if (7u64..=9u64).contains(n) => TimeUnit::Nanoseconds,
            Some(n) => {
                unsupported_sql_err!(
                    "invalid temporal type precision (expected 1-9, found {})",
                    n
                )
            }
        })
    }

    /// Visit a SQL unary operator.
    ///
    /// e.g. +column or -column
    fn plan_unary_op(
        &self,
        op: &UnaryOperator,
        expr: &sqlparser::ast::Expr,
    ) -> SQLPlannerResult<ExprRef> {
        let expr = self.plan_expr(expr)?;
        Ok(match (op, expr.as_ref()) {
            // simplify the parse tree by special-casing common unary +/- ops
            (UnaryOperator::Plus, Expr::Literal(LiteralValue::Int64(n))) => lit(*n),
            (UnaryOperator::Plus, Expr::Literal(LiteralValue::Float64(n))) => lit(*n),
            (UnaryOperator::Minus, Expr::Literal(LiteralValue::Int64(n))) => lit(-n),
            (UnaryOperator::Minus, Expr::Literal(LiteralValue::Float64(n))) => lit(-n),
            // general case
            (UnaryOperator::Plus, _) => lit(0).add(expr),
            (UnaryOperator::Minus, _) => lit(0).sub(expr),
            (UnaryOperator::Not, _) => expr.not(),
            other => unsupported_sql_err!("unary operator {:?}", other),
        })
    }
    fn plan_subscript(
        &self,
        expr: &sqlparser::ast::Expr,
        subscript: &Subscript,
    ) -> SQLPlannerResult<ExprRef> {
        match subscript {
            Subscript::Index { index } => {
                let expr = self.plan_expr(expr)?;
                let index = self.plan_expr(index)?;
                let schema = self
                    .current_relation
                    .as_ref()
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("subscript without a current relation")
                    })
                    .map(Relation::schema)?;
                let expr_field = expr.to_field(schema.as_ref())?;
                match expr_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        Ok(daft_functions::list::get(expr, index, null_lit()))
                    }
                    DataType::Struct(_) => {
                        if let Some(s) = index.as_literal().and_then(|l| l.as_str()) {
                            Ok(daft_dsl::functions::struct_::get(expr, s))
                        } else {
                            invalid_operation_err!("Index must be a string literal")
                        }
                    }
                    DataType::Map { .. } => Ok(daft_dsl::functions::map::get(expr, index)),
                    dtype => {
                        invalid_operation_err!("nested access on column with type: {}", dtype)
                    }
                }
            }
            Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if stride.is_some() {
                    unsupported_sql_err!("stride cannot be provided when slicing an expression");
                }
                match (lower_bound, upper_bound) {
                    (Some(lower), Some(upper)) => {
                        let lower = self.plan_expr(lower)?;
                        let upper = self.plan_expr(upper)?;
                        let expr = self.plan_expr(expr)?;
                        Ok(daft_functions::list::slice(expr, lower, upper))
                    }
                    _ => {
                        unsupported_sql_err!("slice with only one bound not yet supported");
                    }
                }
            }
        }
    }
}

/// Checks if the SQL query is valid syntax and doesn't use unsupported features.
/// /// This function examines various clauses and options in the provided [sqlparser::ast::Query]
/// and returns an error if any unsupported features are encountered.
fn check_query_features(query: &sqlparser::ast::Query) -> SQLPlannerResult<()> {
    if !query.limit_by.is_empty() {
        unsupported_sql_err!("LIMIT BY");
    }
    if query.offset.is_some() {
        unsupported_sql_err!("OFFSET");
    }
    if query.fetch.is_some() {
        unsupported_sql_err!("FETCH");
    }
    if !query.locks.is_empty() {
        unsupported_sql_err!("LOCKS");
    }
    if let Some(for_clause) = &query.for_clause {
        unsupported_sql_err!("{for_clause}");
    }
    if query.settings.is_some() {
        unsupported_sql_err!("SETTINGS");
    }
    if let Some(format_clause) = &query.format_clause {
        unsupported_sql_err!("{format_clause}");
    }
    Ok(())
}

/// Checks if the features used in the SQL SELECT statement are supported.
///
/// This function examines various clauses and options in the provided [sqlparser::ast::Select]
/// and returns an error if any unsupported features are encountered.
///
/// # Arguments
///
/// * `selection` - A reference to the [sqlparser::ast::Select] to be checked.
///
/// # Returns
///
/// * `SQLPlannerResult<()>` - Ok(()) if all features are supported, or an error describing
///   the first unsupported feature encountered.
fn check_select_features(selection: &sqlparser::ast::Select) -> SQLPlannerResult<()> {
    if selection.top.is_some() {
        unsupported_sql_err!("TOP");
    }

    if selection.into.is_some() {
        unsupported_sql_err!("INTO");
    }
    if !selection.lateral_views.is_empty() {
        unsupported_sql_err!("LATERAL");
    }
    if selection.prewhere.is_some() {
        unsupported_sql_err!("PREWHERE");
    }
    if !selection.cluster_by.is_empty() {
        unsupported_sql_err!("CLUSTER BY");
    }
    if !selection.distribute_by.is_empty() {
        unsupported_sql_err!("DISTRIBUTE BY");
    }
    if !selection.sort_by.is_empty() {
        unsupported_sql_err!("SORT BY");
    }

    if !selection.named_window.is_empty() {
        unsupported_sql_err!("WINDOW");
    }
    if selection.qualify.is_some() {
        unsupported_sql_err!("QUALIFY");
    }
    if selection.connect_by.is_some() {
        unsupported_sql_err!("CONNECT BY");
    }
    Ok(())
}

fn check_wildcard_options(
    WildcardAdditionalOptions {
        opt_ilike,
        opt_except,
        opt_replace,
        opt_rename,
        opt_exclude: _,
    }: &WildcardAdditionalOptions,
) -> SQLPlannerResult<()> {
    if opt_ilike.is_some() {
        unsupported_sql_err!("ILIKE");
    }
    if opt_except.is_some() {
        unsupported_sql_err!("EXCEPT");
    }
    if opt_replace.is_some() {
        unsupported_sql_err!("REPLACE");
    }
    if opt_rename.is_some() {
        unsupported_sql_err!("RENAME");
    }

    Ok(())
}
pub fn sql_expr<S: AsRef<str>>(s: S) -> SQLPlannerResult<ExprRef> {
    let mut planner = SQLPlanner::default();

    let tokens = Tokenizer::new(&GenericDialect {}, s.as_ref()).tokenize()?;

    let mut parser = Parser::new(&GenericDialect {})
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_tokens(tokens);

    let expr = parser.parse_select_item()?;
    let empty_schema = Schema::empty();
    let exprs = planner.select_item_to_expr(&expr, &empty_schema)?;
    if exprs.len() != 1 {
        invalid_operation_err!("expected a single expression, found {}", exprs.len())
    }
    Ok(exprs.into_iter().next().unwrap())
}

// ----------------
// Helper functions
// ----------------
fn ident_to_str(ident: &Ident) -> String {
    if ident.quote_style == Some('"') {
        ident.value.to_string()
    } else {
        ident.to_string()
    }
}

fn idents_to_str(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(ident_to_str)
        .collect::<Vec<_>>()
        .join(".")
}

fn get_idents_vec(expr: &sqlparser::ast::Expr) -> SQLPlannerResult<Cow<Vec<Ident>>> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Ok(Cow::Owned(vec![ident.clone()])),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => Ok(Cow::Borrowed(idents)),
        _ => invalid_operation_err!("expected an identifier"),
    }
}

/// unresolves an alias in a projection
/// Example:
/// ```sql
/// SELECT a as b, c FROM t group by b
/// ```
/// in this case if you tried to unresolve the expr `b` using the projections [`a as b`, `c`] you would get `a`
///
/// Since sql allows you to use the alias in the group by or the order by clause, we need to unresolve the alias to the original expression
/// ex:
/// All of the following are valid sql queries
/// `select a as b, c from t group by b`
/// `select a as b, c from t group by a`
/// `select a as b, c from t group by a order by a`
/// `select a as b, c from t group by a order by b`
/// `select a as b, c from t group by b order by a`
/// `select a as b, c from t group by b order by b`
///
/// In all of the above cases, the group by and order by clauses are resolved to the original expression `a`
///
/// This is needed for resolving group by and order by clauses
fn unresolve_alias(expr: ExprRef, projection: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    projection
        .iter()
        .find_map(|p| {
            if let Expr::Alias(e, alias) = &p.as_ref() {
                if expr.name() == alias.as_ref() {
                    Some(e.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .ok_or_else(|| PlannerError::column_not_found(expr.name(), "projection"))
}
