use pyo3::prelude::*;
mod datatype;
mod error;
mod expr;
mod field;
mod schema;
mod series;
mod table;

pub use datatype::PyDataType;
pub use series::PySeries;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<expr::PyExpr>()?;
    parent.add_class::<table::PyTable>()?;
    parent.add_class::<series::PySeries>()?;
    parent.add_class::<datatype::PyDataType>()?;
    parent.add_class::<schema::PySchema>()?;
    parent.add_class::<field::PyField>()?;

    parent.add_wrapped(wrap_pyfunction!(expr::col))?;
    parent.add_wrapped(wrap_pyfunction!(expr::lit))?;
    parent.add_wrapped(wrap_pyfunction!(expr::udf))?;
    parent.add_wrapped(wrap_pyfunction!(expr::eq))?;

    Ok(())
}
