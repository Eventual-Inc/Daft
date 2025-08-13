use ndarray::ArrayD;
#[cfg(feature = "python")]
use pyo3::{Bound, PyAny, Python};

/// Trait to allow dynamic dispatch over ndarray arrays of any type.
pub trait NdArray {
    #[cfg(feature = "python")]
    fn into_py(self: Box<Self>, py: Python) -> Bound<PyAny>;
}

#[cfg(not(feature = "python"))]
impl<A> NdArray for ArrayD<A> {}

#[cfg(feature = "python")]
impl<A> NdArray for ArrayD<A>
where
    A: numpy::Element,
{
    fn into_py(self: Box<Self>, py: Python) -> Bound<PyAny> {
        use numpy::IntoPyArray;

        self.into_pyarray(py).into_any()
    }
}
