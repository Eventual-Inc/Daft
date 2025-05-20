use daft_dsl::python::PyExpr;
use daft_hash::HashFunctionKind;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

#[pyfunction]
#[pyo3(name = "struct")]
pub fn to_struct(inputs: Vec<PyExpr>) -> PyResult<PyExpr> {
    let inputs = inputs.into_iter().map(Into::into).collect();
    Ok(crate::to_struct::to_struct(inputs).into())
}

#[pyfunction]
pub fn minhash(
    expr: PyExpr,
    num_hashes: i64,
    ngram_size: i64,
    seed: i64,
    hash_function: &str,
) -> PyResult<PyExpr> {
    let hash_function: HashFunctionKind = hash_function.parse()?;

    if num_hashes <= 0 {
        return Err(PyValueError::new_err(format!(
            "num_hashes must be positive: {num_hashes}"
        )));
    }
    if ngram_size <= 0 {
        return Err(PyValueError::new_err(format!(
            "ngram_size must be positive: {ngram_size}"
        )));
    }
    let cast_seed = seed as u32;

    let expr = crate::minhash::minhash(
        expr.into(),
        num_hashes as usize,
        ngram_size as usize,
        cast_seed,
        hash_function,
    );
    Ok(expr.into())
}

#[pyfunction(signature = (expr, seed=None))]
pub fn hash(expr: PyExpr, seed: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(crate::hash::hash(expr.into(), seed.map(Into::into)).into())
}
