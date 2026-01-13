use common_error::{DaftError, DaftResult};
use daft_schema::dtype::DataType;

use crate::prelude::{BinaryArray, FixedSizeBinaryArray};

fn add_nullable(lval: Option<&[u8]>, rval: Option<&[u8]>) -> Option<Vec<u8>> {
    if let Some(l) = lval
        && let Some(r) = rval
    {
        Some([l, r].concat())
    } else {
        None
    }
}

pub fn add_binary_arrays(lhs: &BinaryArray, rhs: &BinaryArray) -> DaftResult<BinaryArray> {
    let name = lhs.name();

    Ok(if lhs.len() == 1 {
        let lval = lhs.get(0);
        BinaryArray::from_iter(name, rhs.into_iter().map(|rval| add_nullable(lval, rval)))
    } else if rhs.len() == 1 {
        let rval = rhs.get(0);
        BinaryArray::from_iter(name, lhs.into_iter().map(|lval| add_nullable(lval, rval)))
    } else {
        if lhs.len() != rhs.len() {
            return Err(DaftError::ValueError(format!(
                "Array length mismatch when adding binary arrays: {} != {}",
                lhs.len(),
                rhs.len()
            )));
        }

        BinaryArray::from_iter(
            name,
            lhs.into_iter()
                .zip(rhs)
                .map(|(lval, rval)| add_nullable(lval, rval)),
        )
    })
}

pub fn add_fixed_size_binary_arrays(
    lhs: &FixedSizeBinaryArray,
    rhs: &FixedSizeBinaryArray,
) -> DaftResult<FixedSizeBinaryArray> {
    let name = lhs.name();
    let lhs_size = if let &DataType::FixedSizeBinary(size) = lhs.data_type() {
        size
    } else {
        unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)");
    };

    let rhs_size = if let &DataType::FixedSizeBinary(size) = rhs.data_type() {
        size
    } else {
        unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)");
    };

    let combined_size = lhs_size + rhs_size;

    Ok(if lhs.len() == 1 {
        let lval = lhs.get(0);
        FixedSizeBinaryArray::from_iter(
            name,
            rhs.into_iter().map(|rval| add_nullable(lval, rval)),
            combined_size,
        )
    } else if rhs.len() == 1 {
        let rval = rhs.get(0);
        FixedSizeBinaryArray::from_iter(
            name,
            lhs.into_iter().map(|lval| add_nullable(lval, rval)),
            combined_size,
        )
    } else {
        if lhs.len() != rhs.len() {
            return Err(DaftError::ValueError(format!(
                "Array length mismatch when adding fixed size binary arrays: {} != {}",
                lhs.len(),
                rhs.len()
            )));
        }

        FixedSizeBinaryArray::from_iter(
            name,
            lhs.into_iter()
                .zip(rhs)
                .map(|(lval, rval)| add_nullable(lval, rval)),
            combined_size,
        )
    })
}
