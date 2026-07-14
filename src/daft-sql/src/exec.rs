use std::{collections::HashMap, sync::Arc};

use daft_context::partition_cache::logical_plan_from_micropartitions;
use daft_core::{prelude::Utf8Array, series::IntoSeries};
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_session::Session;

use crate::{
    SQLPlanner,
    error::{PlannerError, SQLPlannerResult},
    statement::{self, Statement},
    unsupported_sql_err,
};

/// Execute result is always a dataframe.
pub(crate) type DataFrame = Arc<LogicalPlan>;

/// Execute SQL statements against the session.
pub(crate) fn execute_statement(
    sess: &Session,
    statement: &str,
    ctes: HashMap<String, LogicalPlanBuilder>,
) -> SQLPlannerResult<Option<DataFrame>> {
    let stmt = SQLPlanner::new(sess).with_ctes(ctes).plan(statement)?;
    match stmt {
        Statement::Select(select) => execute_select(sess, select),
        Statement::Set(set) => execute_set(sess, set),
        Statement::Use(use_) => execute_use(sess, use_),
        Statement::ShowTables(show_tables) => execute_show_tables(sess, show_tables),
        Statement::CreateTable(create_table) => execute_create_table(sess, create_table),
    }
}

fn execute_select(_: &Session, select: DataFrame) -> SQLPlannerResult<Option<DataFrame>> {
    Ok(Some(select))
}

fn execute_set(_: &Session, _: statement::Set) -> SQLPlannerResult<Option<DataFrame>> {
    unsupported_sql_err!("SET statement")
}

fn execute_use(sess: &Session, use_: statement::Use) -> SQLPlannerResult<Option<DataFrame>> {
    sess.set_catalog(Some(&use_.catalog))?;
    sess.set_namespace(use_.namespace.as_ref())?;
    Ok(None)
}

fn execute_create_table(
    sess: &Session,
    create_table: statement::CreateTable,
) -> SQLPlannerResult<Option<DataFrame>> {
    let name = &create_table.name;
    let schema = create_table.schema;

    // Resolve the catalog and identifier.
    let (catalog, ident) = if name.has_qualifier() {
        if sess.has_catalog(name.get(0)) {
            // Catalog-qualified: dispatch to the named catalog.
            let catalog = sess.get_catalog(name.get(0))?;
            let ident = name.drop(1);
            (catalog, ident)
        } else if name.len() >= 3 {
            // 3+ parts where the first is not a known catalog: error.
            return Err(PlannerError::invalid_operation(format!(
                "Catalog '{}' not found",
                name.get(0)
            )));
        } else {
            // 2-part name where the first is not a catalog: schema-qualified.
            let catalog = sess.current_catalog()?.ok_or_else(|| {
                PlannerError::invalid_operation(
                    "Cannot create a table without a current catalog. Use 'USE <catalog>' or provide a catalog-qualified name.".to_string(),
                )
            })?;
            // Keep the full name as ident (schema.table).
            (catalog, name.clone())
        }
    } else {
        // Unqualified: prepend the current namespace.
        let catalog = sess.current_catalog()?.ok_or_else(|| {
            PlannerError::invalid_operation(
                "Cannot create a table without a current catalog. Use 'USE <catalog>' or provide a catalog-qualified name.".to_string(),
            )
        })?;
        let ident = {
            let namespace = sess.current_namespace()?;
            if let Some(ref namespace) = namespace {
                if !namespace.is_empty() {
                    name.qualify(namespace.clone())
                } else {
                    name.clone()
                }
            } else {
                name.clone()
            }
        };
        (catalog, ident)
    };

    // Handle IF NOT EXISTS.
    if create_table.if_not_exists && catalog.has_table(&ident)? {
        return Ok(None);
    }

    // Create the table.
    catalog.create_table(&ident, Arc::new(schema))?;

    Ok(None)
}

fn execute_show_tables(
    sess: &Session,
    show_tables: statement::ShowTables,
) -> SQLPlannerResult<Option<DataFrame>> {
    // lookup or use current schema
    let catalog = match show_tables.catalog {
        Some(catalog) => sess.get_catalog(&catalog)?,
        None => sess.current_catalog()?.ok_or_else(|| {
            PlannerError::invalid_operation(
                "No catalog is currently set. Use 'USE <catalog>' to set a catalog.".to_string(),
            )
        })?,
    };

    // this returns identifiers which we need to split into our columns
    let tables = catalog.list_tables(show_tables.pattern.as_deref())?;

    // these are typical `show` columns which are simplififed INFORMATION_SCHEMA.TABLES columns
    use daft_core::prelude::{DataType, Field, Schema};
    let schema = Schema::new(vec![
        Field::new("catalog", DataType::Utf8),
        Field::new("namespace", DataType::Utf8),
        Field::new("table", DataType::Utf8),
    ]);

    // build the result set

    let mut cat_array = Vec::with_capacity(tables.len());
    let mut nsp_array = Vec::with_capacity(tables.len());
    let mut tbl_array = Vec::with_capacity(tables.len());

    for ident in &tables {
        cat_array.push(Some(catalog.name()));
        if let Some(namespace) = ident.qualifier() {
            nsp_array.push(Some(namespace.join(".")));
        } else {
            nsp_array.push(None);
        }
        tbl_array.push(Some(ident.name().to_string()));
    }
    let cat_array = Utf8Array::from_iter("catalog", cat_array.into_iter());

    let nsp_array = Utf8Array::from_iter("namespace", nsp_array.into_iter());

    let tbl_array = Utf8Array::from_iter("table", tbl_array.into_iter());

    let rb = RecordBatch::from_nonempty_columns(vec![
        cat_array.into_series(),
        nsp_array.into_series(),
        tbl_array.into_series(),
    ])?;

    let part = MicroPartition::new_loaded(schema.into(), Arc::new(vec![rb]), None);

    let scan = logical_plan_from_micropartitions(vec![part])?;

    Ok(Some(scan.build()))
}
