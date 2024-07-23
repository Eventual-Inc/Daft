use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_dsl::{col, Expr, ExprRef};
use daft_plan::{logical_plan::Source, LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::{
    ast::{Query, SelectItem, TableWithJoins},
    dialect::GenericDialect,
};

pub struct SQLContext {
    tables: HashMap<String, Source>,
}
impl Default for SQLContext {
    fn default() -> Self {
        SQLContext {
            tables: HashMap::new(),
        }
    }
}

impl SQLContext {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn get_table(&self, name: &str) -> DaftResult<&Source> {
        self.tables
            .get(name)
            .ok_or(DaftError::ComputeError(format!("Table {} not found", name)))
    }
    pub fn register_table(&mut self, name: &str, table: Source) {
        self.tables.insert(name.to_string(), table);
    }
}

pub struct SQLToLogicalPlan {
    context: SQLContext,
}

impl Default for SQLToLogicalPlan {
    fn default() -> Self {
        SQLToLogicalPlan {
            context: SQLContext::new(),
        }
    }
}

impl SQLToLogicalPlan {
    
    pub fn new(context: SQLContext) -> Self {
        SQLToLogicalPlan { context }
    }
    
    
    pub fn parse_sql(&self, sql: &str) -> DaftResult<LogicalPlanRef> {
        let ast = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &sql)
            .expect("Failed to parse SQL");
        match ast.as_slice() {
            [sqlparser::ast::Statement::Query(query)] => Ok(self.parse_query(query)?.build()),
            _ => todo!(),
        }
    }

    fn parse_query(&self, query: &Query) -> DaftResult<LogicalPlanBuilder> {
        let selection = query.body.as_select().expect("Expected SELECT statement");
        self.parse_select(selection)
    }

    fn parse_select(&self, selection: &sqlparser::ast::Select) -> DaftResult<LogicalPlanBuilder> {
        let from = selection.clone().from;
        if from.len() > 1 {
            return Err(DaftError::ComputeError(
                "Only one table is supported".to_string(),
            ));
        }
        let mut builder = self.parse_from(&from[0])?;
        if selection.projection.len() > 0 {
            let to_select = selection
                .projection
                .iter()
                .map(|expr| self.parse_select_item(expr))
                .collect::<DaftResult<Vec<_>>>()?;

            builder = builder.select(to_select)?;
        }
        if let Some(selection) = &selection.selection {
            println!("selection = {:?}", selection);
            todo!()
        }
        Ok(builder)
    }

    fn parse_from(&self, from: &TableWithJoins) -> DaftResult<LogicalPlanBuilder> {
        let table_factor = from.relation.clone();
        match table_factor {
            ref tbl @ sqlparser::ast::TableFactor::Table { .. } => self.parse_table(tbl),
            _ => todo!(),
        }
    }
    
    fn parse_table(&self, table: &sqlparser::ast::TableFactor) -> DaftResult<LogicalPlanBuilder> {
        let sqlparser::ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
        } = table
        else {
            unreachable!()
        };
        let table_name = name.to_string();
        let source = self.context.get_table(&table_name)?;
        let plan = LogicalPlan::Source(source.clone()).arced();
        let plan_builder = LogicalPlanBuilder::new(plan);
        Ok(plan_builder)
    }

    fn parse_select_item(&self, item: &SelectItem) -> DaftResult<ExprRef> {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.parse_expr(expr)?;
                let alias = alias.value.to_string();
                Ok(expr.alias(alias))
            }
            _ => todo!(),
        }
    }

    fn parse_expr(&self, expr: &sqlparser::ast::Expr) -> DaftResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => Ok(col(ident.to_string())),
            _ => todo!(),
        }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::col;
    use daft_plan::{source_info::PlaceHolderInfo, ClusteringSpec, SourceInfo};
    use sqlparser::{ast::Expr as SQLExpr, dialect::GenericDialect};

    #[test]
    fn test_parse_sql() {
        let mut ctx = SQLContext::new();

        ctx.register_table(
            "tbl",
            Source {
                output_schema: Arc::new(
                    Schema::new(vec![Field::new("test", DataType::Utf8)]).unwrap(),
                ),
                source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                    source_schema: Arc::new(
                        Schema::new(vec![Field::new("test", DataType::Utf8)]).unwrap(),
                    ),
                    clustering_spec: Arc::new(ClusteringSpec::unknown()),
                    source_id: 0,
                })),
            },
        );
        let planner = SQLToLogicalPlan::new(ctx);
        let sql = "select test as a from tbl";
        let plan = planner.parse_sql(sql).unwrap();

        // let plan = parse_sql(sql).unwrap();
        println!("plan = {:?}", plan);
    }
}
