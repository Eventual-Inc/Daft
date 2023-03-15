use crate::{
    dsl::{AggExpr, Expr},
    error::DaftResult,
    table::Table,
};

impl Table {
    pub fn agg(&self, to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[(Expr, &str)]) -> DaftResult<Table> {
        // Convert the input (child, name) exprs to the enum form.
        //  e.g. (expr, "sum") to Expr::Agg(AggEXpr::Sum(expr))
        // (We could do this at the pyo3 layer but I'm not sure why they're strings in the first place)
        let agg_expr_list = to_agg
            .iter()
            .map(|(e, s)| AggExpr::from_name_and_child_expr(s, e))
            .collect::<DaftResult<Vec<AggExpr>>>()?;
        let expr_list = agg_expr_list
            .iter()
            .map(|ae| Expr::Agg(ae.clone()))
            .collect::<Vec<Expr>>();
        self.eval_expression_list(&expr_list)
    }

    pub fn agg_groupby(&self, _to_agg: &[(Expr, &str)], _group_by: &[Expr]) -> DaftResult<Table> {
        todo!()
    }
}
