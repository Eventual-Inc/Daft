use common_error::DaftResult;

use crate::expr::Expr;

/// Window function for computing rank
pub fn rank(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement rank window function
    todo!("Implement rank window function")
}

/// Window function for computing dense rank
pub fn dense_rank(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement dense rank window function
    todo!("Implement dense rank window function")
}

/// Window function for computing row number
pub fn row_number(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement row number window function
    todo!("Implement row number window function")
}

/// Window function for accessing previous row values
pub fn lag(_expr: Expr, _offset: i64, _default: Option<Expr>) -> DaftResult<Expr> {
    // TODO: Implement lag window function
    todo!("Implement lag window function")
}

/// Window function for accessing next row values
pub fn lead(_expr: Expr, _offset: i64, _default: Option<Expr>) -> DaftResult<Expr> {
    // TODO: Implement lead window function
    todo!("Implement lead window function")
}

/// Window function for getting first value in frame
pub fn first_value(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement first value window function
    todo!("Implement first value window function")
}

/// Window function for getting last value in frame
pub fn last_value(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement last value window function
    todo!("Implement last value window function")
}

/// Window function for getting nth value in frame
pub fn nth_value(_expr: Expr, _n: i64) -> DaftResult<Expr> {
    // TODO: Implement nth value window function
    todo!("Implement nth value window function")
}

/// Window function for computing percent rank
pub fn percent_rank(_expr: Expr) -> DaftResult<Expr> {
    // TODO: Implement percent rank window function
    todo!("Implement percent rank window function")
}

/// Window function for computing ntile
pub fn ntile(_expr: Expr, _n: i64) -> DaftResult<Expr> {
    // TODO: Implement ntile window function
    todo!("Implement ntile window function")
}
