use common_error::DaftResult;
use daft_schema::dtype::DataType;

use crate::{array::ops::DaftLogical, series::Series};

macro_rules! binary_op_not_implemented {
    ($self:expr, $rhs:expr, $op:ident) => {{
        let left_dtype = $self.data_type();
        let right_dtype = $rhs.data_type();
        let op_name = stringify!($op);
        return Err(common_error::DaftError::ComputeError(format!(
            "Binary Op: {op_name} not implemented for {left_dtype}, {right_dtype}"
        )));
    }};
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<Series>;

    fn and(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, and),
        }
    }

    fn or(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, or),
        }
    }

    fn xor(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            #[cfg(feature = "python")]
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, xor),
        }
    }
}
