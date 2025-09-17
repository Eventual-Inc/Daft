use arrow2::{
    array::{BinaryArray, FixedSizeBinaryArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
};
use common_error::{DaftError, DaftResult};

pub fn add_binary_arrays(
    lhs: &BinaryArray<i64>,
    rhs: &BinaryArray<i64>,
) -> DaftResult<BinaryArray<i64>> {
    fn add_nullable(lval: Option<&[u8]>, rval: Option<&[u8]>) -> Option<Vec<u8>> {
        if let Some(l) = lval
            && let Some(r) = rval
        {
            Some([l, r].concat())
        } else {
            None
        }
    }

    Ok(if lhs.len() == 1 {
        let lval = lhs.get(0);
        rhs.iter()
            .map(|rval| add_nullable(lval, rval))
            .collect::<BinaryArray<i64>>()
    } else if rhs.len() == 1 {
        let rval = rhs.get(0);
        lhs.iter()
            .map(|lval| add_nullable(lval, rval))
            .collect::<BinaryArray<i64>>()
    } else {
        if lhs.len() != rhs.len() {
            return Err(DaftError::ValueError(format!(
                "Array length mismatch when adding binary arrays: {} != {}",
                lhs.len(),
                rhs.len()
            )));
        }

        lhs.iter()
            .zip(rhs)
            .map(|(lval, rval)| add_nullable(lval, rval))
            .collect::<BinaryArray<i64>>()
    })
}

pub fn add_fixed_size_binary_arrays(
    lhs: &FixedSizeBinaryArray,
    rhs: &FixedSizeBinaryArray,
) -> DaftResult<FixedSizeBinaryArray> {
    let combined_size = lhs.size() + rhs.size();

    fn add_nullable(
        lval: Option<&[u8]>,
        rval: Option<&[u8]>,
        values: &mut Vec<u8>,
        validity: &mut MutableBitmap,
        combined_size: usize,
    ) {
        if let Some(l) = lval
            && let Some(r) = rval
        {
            values.extend_from_slice(l);
            values.extend_from_slice(r);
            validity.push(true);
        } else {
            values.extend(std::iter::repeat_n(0u8, combined_size));
            validity.push(false);
        }
    }

    let mut values = Vec::new();
    let mut validity = arrow2::bitmap::MutableBitmap::new();

    if lhs.len() == 1 {
        let lval = lhs.get(0);
        for rval in rhs {
            add_nullable(lval, rval, &mut values, &mut validity, combined_size);
        }
    } else if rhs.len() == 1 {
        let rval = rhs.get(0);
        for lval in lhs {
            add_nullable(lval, rval, &mut values, &mut validity, combined_size);
        }
    } else {
        if lhs.len() != rhs.len() {
            return Err(DaftError::ValueError(format!(
                "Array length mismatch when adding fixed size arrays: {} != {}",
                lhs.len(),
                rhs.len()
            )));
        }

        for (lval, rval) in lhs.iter().zip(rhs) {
            add_nullable(lval, rval, &mut values, &mut validity, combined_size);
        }
    }

    Ok(FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(combined_size),
        values.into(),
        Some(validity.into()),
    ))
}
