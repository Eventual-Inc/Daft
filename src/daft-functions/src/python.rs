use daft_dsl::python::PyExpr;
use daft_hash::HashFunctionKind;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

#[pyfunction]
pub fn cosine_distance(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
    Ok(crate::distance::cosine_distance(a.into(), b.into()).into())
}

#[pyfunction]
pub fn to_struct(inputs: Vec<PyExpr>) -> PyResult<PyExpr> {
    let inputs = inputs.into_iter().map(std::convert::Into::into).collect();
    let expr = crate::to_struct::to_struct(inputs);
    Ok(expr.into())
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

#[pyfunction]
pub fn hash(expr: PyExpr, seed: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(crate::hash::hash(expr.into(), seed.map(Into::into)).into())
}

#[pyfunction]
pub fn utf8_count_matches(
    expr: PyExpr,
    patterns: PyExpr,
    whole_words: bool,
    case_sensitive: bool,
) -> PyResult<PyExpr> {
    let expr = crate::count_matches::utf8_count_matches(
        expr.into(),
        patterns.into(),
        whole_words,
        case_sensitive,
    );
    Ok(expr.into())
}

pub mod float {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn not_nan(expr: PyExpr) -> PyResult<PyExpr> {
        Ok(crate::float::not_nan(expr.into()).into())
    }

    #[pyfunction]
    pub fn is_nan(expr: PyExpr) -> PyResult<PyExpr> {
        Ok(crate::float::is_nan(expr.into()).into())
    }

    #[pyfunction]
    pub fn fill_nan(expr: PyExpr, fill_value: PyExpr) -> PyResult<PyExpr> {
        Ok(crate::float::fill_nan(expr.into(), fill_value.into()).into())
    }

    #[pyfunction]
    pub fn is_inf(expr: PyExpr) -> PyResult<PyExpr> {
        Ok(crate::float::is_inf(expr.into()).into())
    }
}

pub mod uri {
    use daft_dsl::python::PyExpr;
    use daft_io::python::IOConfig as PyIOConfig;
    use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

    #[pyfunction]
    pub fn url_download(
        expr: PyExpr,
        max_connections: i64,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: PyIOConfig,
    ) -> PyResult<PyExpr> {
        if max_connections <= 0 {
            return Err(PyValueError::new_err(format!(
                "max_connections must be positive and non_zero: {max_connections}"
            )));
        }

        Ok(crate::uri::download(
            expr.into(),
            max_connections as usize,
            raise_error_on_failure,
            multi_thread,
            Some(config.config),
        )
        .into())
    }

    #[pyfunction]
    pub fn url_upload(
        expr: PyExpr,
        folder_location: &str,
        max_connections: i64,
        multi_thread: bool,
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyExpr> {
        if max_connections <= 0 {
            return Err(PyValueError::new_err(format!(
                "max_connections must be positive and non_zero: {max_connections}"
            )));
        }
        Ok(crate::uri::upload(
            expr.into(),
            folder_location,
            max_connections as usize,
            multi_thread,
            io_config.map(|io_config| io_config.config),
        )
        .into())
    }
}

pub mod tokenize {
    use daft_dsl::python::PyExpr;
    use daft_io::python::IOConfig as PyIOConfig;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn tokenize_encode(
        expr: PyExpr,
        tokens_path: &str,
        use_special_tokens: bool,
        io_config: Option<PyIOConfig>,
        pattern: Option<&str>,
        special_tokens: Option<&str>,
    ) -> PyResult<PyExpr> {
        Ok(crate::tokenize::tokenize_encode(
            expr.into(),
            tokens_path,
            io_config.map(|x| x.config),
            pattern,
            special_tokens,
            use_special_tokens,
        )
        .into())
    }

    #[pyfunction]
    pub fn tokenize_decode(
        expr: PyExpr,
        tokens_path: &str,
        io_config: Option<PyIOConfig>,
        pattern: Option<&str>,
        special_tokens: Option<&str>,
    ) -> PyResult<PyExpr> {
        Ok(crate::tokenize::tokenize_decode(
            expr.into(),
            tokens_path,
            io_config.map(|x| x.config),
            pattern,
            special_tokens,
        )
        .into())
    }
}
