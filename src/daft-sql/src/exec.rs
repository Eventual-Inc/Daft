use std::{rc::Rc, sync::Arc};

use common_error::DaftResult;
use daft_logical_plan::LogicalPlan;
use daft_session::Session;

use crate::{
    error::PlannerError, statement, statement::Statement, table_provider::in_memory, SQLPlanner,
};

/// Execute result is always a dataframe.
pub(crate) type DataFrame = Arc<LogicalPlan>;

/// Execute SQL statements against the session.
pub(crate) fn execute_statement(sess: Session, statement: &str) -> DaftResult<Option<DataFrame>> {
    let sess: Rc<Session> = Rc::new(sess.into());
    let stmt = SQLPlanner::new(sess.clone()).plan(statement)?;
    match stmt {
        Statement::Select(select) => execute_select(&sess, select),
        Statement::Set(set) => execute_set(&sess, set),
        Statement::Use(use_) => execute_use(&sess, use_),
        Statement::ShowTables(show_tables) => execute_show_tables(&sess, show_tables),
    }
}

fn execute_select(_: &Session, select: DataFrame) -> DaftResult<Option<DataFrame>> {
    Ok(Some(select))
}

fn execute_set(_: &Session, _: statement::Set) -> DaftResult<Option<DataFrame>> {
    Err(PlannerError::unsupported_sql(
        "SET statement is not yet supported.".to_string(),
    ))?
}

fn execute_use(sess: &Session, use_: statement::Use) -> DaftResult<Option<DataFrame>> {
    sess.set_catalog(Some(&use_.catalog))?;
    sess.set_namespace(use_.namespace.as_ref())?;
    Ok(None)
}

fn execute_show_tables(
    sess: &Session,
    show_tables: statement::ShowTables,
) -> DaftResult<Option<DataFrame>> {
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
    let tables = catalog.list_tables(show_tables.pattern)?;

    // these are typical `show` columns which are simplififed INFORMATION_SCHEMA.TABLES columns
    use daft_core::prelude::{DataType, Field, Schema};
    let schema = Schema::new(vec![
        Field::new("catalog", DataType::Utf8),
        Field::new("namespace", DataType::Utf8),
        Field::new("table", DataType::Utf8),
    ])?;

    // build the result set
    use arrow2::array::{MutableArray, MutableUtf8Array};
    let mut cat_array = MutableUtf8Array::<i64>::with_capacity(tables.len());
    let mut nsp_array = MutableUtf8Array::<i64>::with_capacity(tables.len());
    let mut tbl_array = MutableUtf8Array::<i64>::with_capacity(tables.len());
    for ident in &tables {
        cat_array.push(Some(catalog.name()));
        if ident.qualifier.is_empty() {
            nsp_array.push_null();
        } else {
            nsp_array.push(Some(ident.qualifier.join(".")));
        }
        tbl_array.push(Some(ident.name.to_string()));
    }

    // create an in-memory scan arrow arrays
    let scan = in_memory::from_arrow(
        schema,
        vec![cat_array.as_box(), nsp_array.as_box(), tbl_array.as_box()],
    )?;

    Ok(Some(scan.build()))
}
