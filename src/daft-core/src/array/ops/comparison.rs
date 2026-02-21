use std::{
    cmp::Ordering,
    ops::{BitAnd, BitOr, BitXor, Not},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, BooleanBuilder, Datum,
        FixedSizeListArray as ArrowFixedSizeListArray, LargeListArray, ListArray as ArrowListArray,
        PrimitiveArray, StructArray as ArrowStructArray,
    },
    buffer::{BooleanBuffer, NullBuffer},
    compute::kernels::cmp,
};
use arrow_row::{RowConverter, SortField};
use common_error::{DaftError, DaftResult};
use daft_arrow::ArrowError;
use num_traits::{NumCast, ToPrimitive};

use super::{DaftCompare, DaftLogical, as_arrow::AsArrow};
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftArrowBackedType, DaftPrimitiveType, DataType, Field,
        FixedSizeBinaryArray, NumericNative, Utf8Array,
    },
    prelude::Operator,
    series::Series,
};

fn compare_op_matches_ordering(op: Operator, ordering: Ordering) -> bool {
    match op {
        Operator::Eq | Operator::EqNullSafe => ordering == Ordering::Equal,
        Operator::NotEq => ordering != Ordering::Equal,
        Operator::Lt => ordering == Ordering::Less,
        Operator::LtEq => ordering != Ordering::Greater,
        Operator::Gt => ordering == Ordering::Greater,
        Operator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("invalid operator for row comparison"),
    }
}

fn comparison_broadcast_info(
    lhs_name: &str,
    lhs_len: usize,
    rhs_name: &str,
    rhs_len: usize,
) -> DaftResult<(usize, bool, bool)> {
    match (lhs_len, rhs_len) {
        (1, 1) => Ok((1, true, true)),
        (1, r) => Ok((r, true, false)),
        (l, 1) => Ok((l, false, true)),
        (l, r) if l == r => Ok((l, false, false)),
        (l, r) => Err(DaftError::ValueError(format!(
            "trying to compare different length arrays: {lhs_name}: {l} vs {rhs_name}: {r}"
        ))),
    }
}

fn compare_scalar_series(lhs: &Series, rhs: &Series, op: Operator) -> DaftResult<Option<bool>> {
    let result = match op {
        Operator::Eq => lhs.equal(rhs)?,
        Operator::NotEq => lhs.not_equal(rhs)?,
        Operator::Lt => lhs.lt(rhs)?,
        Operator::LtEq => lhs.lte(rhs)?,
        Operator::Gt => lhs.gt(rhs)?,
        Operator::GtEq => lhs.gte(rhs)?,
        Operator::EqNullSafe => lhs.eq_null_safe(rhs)?,
        _ => unreachable!("invalid operator for scalar comparison"),
    };

    Ok(result.get(0))
}

/// Optimization trait for row-based comparison using Arrow's `RowConverter`.
///
/// This trait provides a fast path for comparing arrays by converting them to a row-based
/// format that can be compared efficiently. The fast path is only used when:
/// - Both arrays contain no null values (nulls require special handling)
/// - The data type does not contain floats (NaN requires special handling)
/// - The `RowConverter` supports the data type
///
/// If any of these conditions are not met, the method returns `None` and the caller
/// falls back to element-wise comparison.
trait RowCompareFastPath {
    fn name(&self) -> &str;
    fn to_arrow(&self) -> DaftResult<ArrayRef>;

    fn row_fast_path(
        &self,
        rhs: &Self,
        len: usize,
        lhs_scalar: bool,
        rhs_scalar: bool,
        op: Operator,
    ) -> DaftResult<Option<BooleanArray>> {
        let lhs = self.to_arrow()?;
        let rhs = rhs.to_arrow()?;
        if lhs.data_type() != rhs.data_type() {
            return Ok(None);
        }

        if Self::type_contains_float(lhs.data_type()) {
            return Ok(None);
        }

        if Self::array_has_any_nulls(lhs.as_ref()) || Self::array_has_any_nulls(rhs.as_ref()) {
            return Ok(None);
        }

        let sort_field = SortField::new(lhs.data_type().clone());
        if !RowConverter::supports_fields(std::slice::from_ref(&sort_field)) {
            return Ok(None);
        }

        let converter = RowConverter::new(vec![sort_field]).map_err(DaftError::from)?;
        let lhs_rows = converter.convert_columns(&[lhs]).map_err(DaftError::from)?;
        let rhs_rows = converter.convert_columns(&[rhs]).map_err(DaftError::from)?;

        let mut values = BooleanBuilder::with_capacity(len);
        for idx in 0..len {
            let lhs_idx = if lhs_scalar { 0 } else { idx };
            let rhs_idx = if rhs_scalar { 0 } else { idx };
            values.append_value(compare_op_matches_ordering(
                op,
                lhs_rows.row(lhs_idx).cmp(&rhs_rows.row(rhs_idx)),
            ));
        }

        let arr = BooleanArray::from_builder(self.name(), values);
        Ok(Some(arr))
    }

    fn array_has_any_nulls(array: &dyn Array) -> bool {
        if array.null_count() > 0 {
            return true;
        }

        match array.data_type() {
            arrow::datatypes::DataType::List(_) => {
                let list = array.as_any().downcast_ref::<ArrowListArray>().unwrap();
                Self::array_has_any_nulls(list.values().as_ref())
            }
            arrow::datatypes::DataType::LargeList(_) => {
                let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                Self::array_has_any_nulls(list.values().as_ref())
            }
            arrow::datatypes::DataType::FixedSizeList(_, _) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ArrowFixedSizeListArray>()
                    .unwrap();
                Self::array_has_any_nulls(list.values().as_ref())
            }
            arrow::datatypes::DataType::Struct(_) => {
                let struct_arr = array.as_any().downcast_ref::<ArrowStructArray>().unwrap();
                struct_arr
                    .columns()
                    .iter()
                    .any(|child| Self::array_has_any_nulls(child.as_ref()))
            }
            arrow::datatypes::DataType::Dictionary(_, _) => true,
            arrow::datatypes::DataType::Map(_, _) => true,
            arrow::datatypes::DataType::Union(_, _) => true,
            arrow::datatypes::DataType::RunEndEncoded(_, _) => true,
            _ => false,
        }
    }

    fn type_contains_float(dtype: &arrow::datatypes::DataType) -> bool {
        match dtype {
            arrow::datatypes::DataType::Float16
            | arrow::datatypes::DataType::Float32
            | arrow::datatypes::DataType::Float64 => true,
            arrow::datatypes::DataType::List(field)
            | arrow::datatypes::DataType::LargeList(field)
            | arrow::datatypes::DataType::FixedSizeList(field, _) => {
                Self::type_contains_float(field.data_type())
            }
            arrow::datatypes::DataType::Struct(fields) => fields
                .iter()
                .any(|field| Self::type_contains_float(field.data_type())),
            arrow::datatypes::DataType::Dictionary(_, value) => Self::type_contains_float(value),
            arrow::datatypes::DataType::RunEndEncoded(_, value) => {
                Self::type_contains_float(value.data_type())
            }
            arrow::datatypes::DataType::Union(fields, _) => fields
                .iter()
                .any(|(_, field)| Self::type_contains_float(field.data_type())),
            _ => false,
        }
    }
}

macro_rules! impl_row_compare_fast_path {
    ($array_type:ty) => {
        impl RowCompareFastPath for $array_type {
            fn name(&self) -> &str {
                self.name()
            }

            fn to_arrow(&self) -> DaftResult<ArrayRef> {
                self.to_arrow()
            }
        }
    };
}

impl_row_compare_fast_path!(ListArray);
impl_row_compare_fast_path!(FixedSizeListArray);
impl_row_compare_fast_path!(StructArray);

/// Trait abstracting common operations for list-like arrays (List and FixedSizeList).
///
/// This trait extends `RowCompareFastPath` since all list-like arrays support the
/// row-based comparison optimization. It adds list-specific operations like indexing.
trait ListLikeArray: RowCompareFastPath {
    fn len(&self) -> usize;
    fn get(&self, idx: usize) -> Option<Series>;
}

macro_rules! impl_list_like_array {
    ($array_type:ty) => {
        impl ListLikeArray for $array_type {
            fn len(&self) -> usize {
                self.len()
            }

            fn get(&self, idx: usize) -> Option<Series> {
                self.get(idx)
            }
        }
    };
}

impl_list_like_array!(ListArray);
impl_list_like_array!(FixedSizeListArray);

/// Compares two list series for equality element-wise.
///
/// Returns `Some(true)` if all elements are equal, `Some(false)` if any element differs,
/// or `None` if the result is unknown due to null values.
///
/// When `null_safe` is true, null elements are compared as equal (null == null → true).
/// When `null_safe` is false, comparing with null produces unknown (null == value → None).
fn compare_list_eq(lhs: &Series, rhs: &Series, null_safe: bool) -> DaftResult<Option<bool>> {
    if lhs.len() != rhs.len() {
        return Ok(Some(false));
    }

    let op = if null_safe {
        Operator::EqNullSafe
    } else {
        Operator::Eq
    };

    let mut all_known_equal = true;
    for idx in 0..lhs.len() {
        let lhs_elem = lhs.slice(idx, idx + 1)?;
        let rhs_elem = rhs.slice(idx, idx + 1)?;
        let cmp = compare_scalar_series(&lhs_elem, &rhs_elem, op)?;
        match cmp {
            None => all_known_equal = false,
            Some(true) => {}
            Some(false) => return Ok(Some(false)),
        }
    }

    Ok(all_known_equal.then_some(true))
}

/// Compares two list series using lexicographic ordering.
///
/// Compares elements one by one from left to right. The first differing element determines
/// the result. If all compared elements are equal, the list lengths are compared as a tiebreaker:
/// - For `<` and `<=`: shorter list is less than longer list
/// - For `>` and `>=`: longer list is greater than shorter list
///
/// Returns `None` if any element comparison produces an unknown result (e.g., comparing with null).
///
/// # Examples
/// - `[1, 2] < [1, 3]` → `Some(true)` (second element differs)
/// - `[1, 2] < [1, 2, 3]` → `Some(true)` (all elements equal, shorter list is less)
/// - `[1, null] < [1, 5]` → `None` (comparison with null is unknown)
fn compare_list_order(lhs: &Series, rhs: &Series, op: Operator) -> DaftResult<Option<bool>> {
    let min_len = std::cmp::min(lhs.len(), rhs.len());
    for idx in 0..min_len {
        let lhs_elem = lhs.slice(idx, idx + 1)?;
        let rhs_elem = rhs.slice(idx, idx + 1)?;
        let eq = compare_scalar_series(&lhs_elem, &rhs_elem, Operator::Eq)?;
        match eq {
            None => return Ok(None),
            Some(true) => {}
            Some(false) => {
                let dir_op = match op {
                    Operator::Lt | Operator::LtEq => Operator::Lt,
                    Operator::Gt | Operator::GtEq => Operator::Gt,
                    _ => unreachable!("invalid compare op for ordering"),
                };
                return compare_scalar_series(&lhs_elem, &rhs_elem, dir_op);
            }
        }
    }

    Ok(Some(match op {
        Operator::Lt => lhs.len() < rhs.len(),
        Operator::LtEq => lhs.len() <= rhs.len(),
        Operator::Gt => lhs.len() > rhs.len(),
        Operator::GtEq => lhs.len() >= rhs.len(),
        _ => unreachable!("invalid compare op for ordering"),
    }))
}

fn compare_list_like_value(
    lhs: Option<Series>,
    rhs: Option<Series>,
    op: Operator,
) -> DaftResult<Option<bool>> {
    match op {
        Operator::EqNullSafe => match (lhs, rhs) {
            (None, None) => Ok(Some(true)),
            (None, Some(_)) | (Some(_), None) => Ok(Some(false)),
            (Some(lhs), Some(rhs)) => compare_list_eq(&lhs, &rhs, true),
        },
        Operator::Eq => match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => compare_list_eq(&lhs, &rhs, false),
            _ => Ok(None),
        },
        Operator::NotEq => {
            let eq = compare_list_like_value(lhs, rhs, Operator::Eq)?;
            Ok(eq.map(|v| !v))
        }
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => compare_list_order(&lhs, &rhs, op),
            _ => Ok(None),
        },
        _ => unreachable!("invalid compare op for list values"),
    }
}

fn compare_list_like_array<A: ListLikeArray>(
    lhs: &A,
    rhs: &A,
    op: Operator,
) -> DaftResult<BooleanArray> {
    let (len, lhs_scalar, rhs_scalar) =
        comparison_broadcast_info(lhs.name(), lhs.len(), rhs.name(), rhs.len())?;

    if let Some(result) = lhs.row_fast_path(rhs, len, lhs_scalar, rhs_scalar, op)? {
        return Ok(result);
    }

    let mut results = BooleanBuilder::with_capacity(len);
    for idx in 0..len {
        let lhs_idx = if lhs_scalar { 0 } else { idx };
        let rhs_idx = if rhs_scalar { 0 } else { idx };
        let lhs_value = lhs.get(lhs_idx);
        let rhs_value = rhs.get(rhs_idx);
        results.append_option(compare_list_like_value(lhs_value, rhs_value, op)?);
    }

    Ok(BooleanArray::from_builder(lhs.name(), results))
}

/// Compares two struct values field by field.
///
/// Struct comparison follows these rules:
/// - Fields are compared in the order they appear in the schema
/// - For equality: all fields must be equal
/// - For ordering: the first differing field determines the result
/// - If a struct-level null is encountered, it's handled according to the operator
///   (null-safe equality treats null == null as true, other ops treat it as unknown)
///
/// Returns `Some(true/false)` for definite results, or `None` for unknown (null-dependent) results.
fn compare_struct_value(
    lhs: &StructArray,
    rhs: &StructArray,
    lhs_idx: usize,
    rhs_idx: usize,
    op: Operator,
) -> DaftResult<Option<bool>> {
    let lhs_valid = lhs
        .nulls()
        .map(|nulls| nulls.is_valid(lhs_idx))
        .unwrap_or(true);
    let rhs_valid = rhs
        .nulls()
        .map(|nulls| nulls.is_valid(rhs_idx))
        .unwrap_or(true);

    match op {
        Operator::EqNullSafe => {
            if !lhs_valid || !rhs_valid {
                return Ok(Some(!lhs_valid && !rhs_valid));
            }
        }
        _ => {
            if !lhs_valid || !rhs_valid {
                return Ok(None);
            }
        }
    }

    let is_not_eq = matches!(op, Operator::NotEq);
    let (eq_op, order_op) = match op {
        Operator::Eq => (Operator::Eq, None),
        Operator::EqNullSafe => (Operator::EqNullSafe, None),
        Operator::NotEq => (Operator::Eq, None),
        Operator::Lt | Operator::LtEq => (Operator::Eq, Some(Operator::Lt)),
        Operator::Gt | Operator::GtEq => (Operator::Eq, Some(Operator::Gt)),
        _ => unreachable!("invalid compare op for struct value"),
    };

    let mut all_known_equal = true;
    for (lhs_child, rhs_child) in lhs.children.iter().zip(rhs.children.iter()) {
        let lhs_elem = lhs_child.slice(lhs_idx, lhs_idx + 1)?;
        let rhs_elem = rhs_child.slice(rhs_idx, rhs_idx + 1)?;
        let eq = compare_scalar_series(&lhs_elem, &rhs_elem, eq_op)?;
        match eq {
            None => {
                if order_op.is_some() {
                    return Ok(None);
                }
                all_known_equal = false;
            }
            Some(true) => {}
            Some(false) => {
                if let Some(order_op) = order_op {
                    return compare_scalar_series(&lhs_elem, &rhs_elem, order_op);
                }
                return Ok(Some(is_not_eq));
            }
        }
    }

    if !all_known_equal {
        return Ok(None);
    }

    Ok(Some(match op {
        Operator::Eq | Operator::EqNullSafe => true,
        Operator::NotEq => false,
        Operator::Lt | Operator::Gt => false,
        Operator::LtEq | Operator::GtEq => true,
        _ => unreachable!("invalid compare op for struct value"),
    }))
}

fn compare_struct_array(
    lhs: &StructArray,
    rhs: &StructArray,
    op: Operator,
) -> DaftResult<BooleanArray> {
    let (len, lhs_scalar, rhs_scalar) =
        comparison_broadcast_info(lhs.name(), lhs.len(), rhs.name(), rhs.len())?;

    if let Some(result) = lhs.row_fast_path(rhs, len, lhs_scalar, rhs_scalar, op)? {
        return Ok(result);
    }

    let mut results = BooleanBuilder::with_capacity(len);
    for idx in 0..len {
        let lhs_idx = if lhs_scalar { 0 } else { idx };
        let rhs_idx = if rhs_scalar { 0 } else { idx };
        results.append_option(compare_struct_value(lhs, rhs, lhs_idx, rhs_idx, op)?);
    }
    Ok(BooleanArray::from_builder(lhs.name(), results))
}

impl<T> PartialEq for DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.to_arrow().eq(&other.to_arrow())
    }
}

impl<T> DataArray<T> {
    fn compare_op(
        &self,
        rhs: &Self,
        op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
    ) -> DaftResult<BooleanArray> {
        let arrow_arr = match (self.len(), rhs.len()) {
            (1, 1) => op(
                &arrow::array::Scalar::new(self.to_arrow()),
                &arrow::array::Scalar::new(rhs.to_arrow()),
            )?,
            (1, _) => op(&arrow::array::Scalar::new(self.to_arrow()), &rhs.to_arrow())?,
            (_, 1) => op(&self.to_arrow(), &arrow::array::Scalar::new(rhs.to_arrow()))?,
            (l, r) if l == r => op(&self.to_arrow(), &rhs.to_arrow())?,
            (l, r) => {
                return Err(DaftError::ValueError(format!(
                    "trying to compare different length arrays: {}: {l} vs {}: {r}",
                    self.name(),
                    rhs.name()
                )));
            }
        };

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl<T> DaftCompare<&Self> for DataArray<T> {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::eq)
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        if self.data_type().is_null() {
            let len = match (self.len(), rhs.len()) {
                (1, len) | (len, 1) => len,
                (l, r) if l == r => l,
                (l, r) => {
                    return Err(DaftError::ValueError(format!(
                        "trying to compare different length arrays: {}: {l} vs {}: {r}",
                        self.name(),
                        rhs.name()
                    )));
                }
            };

            return Ok(BooleanArray::from_values(
                self.name(),
                std::iter::repeat_n(true, len),
            ));
        }

        let eq_arr = self
            .with_nulls(None)?
            .compare_op(&rhs.with_nulls(None)?, cmp::eq)?;

        match (self.nulls(), rhs.nulls()) {
            (Some(l_valid), Some(r_valid)) => {
                let l_valid = BooleanArray::from_null_buffer("", l_valid)?;
                let r_valid = BooleanArray::from_null_buffer("", r_valid)?;

                // (E & L & R) | (!L & !R)
                (eq_arr.and(&l_valid)?.and(&r_valid)?).or(&l_valid.not()?.and(&r_valid.not()?)?)
            }
            (Some(valid), None) | (None, Some(valid)) => {
                let valid = BooleanArray::from_null_buffer("", valid)?;

                eq_arr.and(&valid)
            }
            (None, None) => Ok(eq_arr),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::neq)
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::lt)
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::lt_eq)
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::gt)
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::gt_eq)
    }
}

macro_rules! impl_nested_compare {
    ($array_type:ty, $compare_fn:ident) => {
        impl DaftCompare<&Self> for $array_type {
            type Output = DaftResult<BooleanArray>;

            fn equal(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::Eq)
            }

            fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::EqNullSafe)
            }

            fn not_equal(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::NotEq)
            }

            fn lt(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::Lt)
            }

            fn lte(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::LtEq)
            }

            fn gt(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::Gt)
            }

            fn gte(&self, rhs: &Self) -> Self::Output {
                $compare_fn(self, rhs, Operator::GtEq)
            }
        }
    };
}

impl_nested_compare!(ListArray, compare_list_like_array);
impl_nested_compare!(FixedSizeListArray, compare_list_like_array);
impl_nested_compare!(StructArray, compare_struct_array);

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: NumCast,
{
    fn compare_to_scalar(
        &self,
        rhs: impl ToPrimitive,
        op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
    ) -> DaftResult<BooleanArray> {
        let rhs: <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let rhs = PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::new_scalar(rhs);

        let arrow_arr = op(&self.to_arrow(), &rhs)?;

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl<T, Scalar> DaftCompare<Scalar> for DataArray<T>
where
    T: DaftPrimitiveType,
    Scalar: ToPrimitive,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: NumCast,
{
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::eq)
    }

    fn not_equal(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::neq)
    }

    fn lt(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::lt)
    }

    fn lte(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::lt_eq)
    }

    fn gt(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::gt)
    }

    fn gte(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::gt_eq)
    }

    fn eq_null_safe(&self, rhs: Scalar) -> Self::Output {
        // we can simply use the equality kernel then set all nulls to false.
        // this is because the type constraints ensure that the scalar is never null
        let eq_arr = self.compare_to_scalar(rhs, cmp::eq)?;

        Ok(if let Some(nulls) = eq_arr.nulls() {
            let valid_arr = BooleanArray::from_null_buffer("", nulls)?;
            eq_arr.with_nulls(None)?.and(&valid_arr)?
        } else {
            eq_arr
        })
    }
}

impl Not for &BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn not(self) -> Self::Output {
        let arrow_arr = arrow::compute::not(&self.as_arrow()?)?;

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl DaftLogical<&Self> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: &Self) -> Self::Output {
        // When performing a logical AND with a NULL value:
        // - If the non-null value is false, the result is false (not null)
        // - If the non-null value is true, the result is null
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.and(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.and(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitand(r_values);

                let nulls = match (self.nulls(), rhs.nulls()) {
                    (None, None) => None,
                    (Some(l_valid), None) => {
                        let l_valid = l_valid.inner();

                        Some(
                            l_valid
                                .bitor(&l_values.not().bitand(l_valid))
                                .bitor(&r_values.not()),
                        )
                    }
                    (None, Some(r_valid)) => {
                        let r_valid = r_valid.inner();

                        Some(
                            r_valid
                                .bitor(&l_values.not())
                                .bitor(&r_values.not().bitand(r_valid)),
                        )
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let l_valid = l_valid.inner();
                        let r_valid = r_valid.inner();

                        Some(
                            (l_valid.bitand(r_valid))
                                .bitor(&l_values.not().bitand(l_valid))
                                .bitor(&r_values.not().bitand(r_valid)),
                        )
                    }
                };

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(
                        values,
                        nulls.map(Into::into),
                    )),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        // When performing a logical OR with a NULL value:
        // - If the non-null value is false, the result is null
        // - If the non-null value is true, the result is true (not null)
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.or(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.or(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitor(r_values);

                let nulls = match (self.nulls(), rhs.nulls()) {
                    (None, None) => None,
                    (Some(l_valid), None) => {
                        let l_valid = l_valid.inner();

                        Some(l_valid.bitor(&l_values.bitand(l_valid)).bitor(r_values))
                    }
                    (None, Some(r_valid)) => {
                        let r_valid = r_valid.inner();

                        Some(r_valid.bitor(l_values).bitor(&r_values.bitand(r_valid)))
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let l_valid = l_valid.inner();
                        let r_valid = r_valid.inner();

                        Some(
                            (l_valid.bitand(r_valid))
                                .bitor(&l_values.bitand(l_valid))
                                .bitor(&r_values.bitand(r_valid)),
                        )
                    }
                };

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(
                        values,
                        nulls.map(Into::into),
                    )),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.xor(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.xor(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitxor(r_values);
                let nulls = NullBuffer::union(self.nulls(), rhs.nulls());

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(values, nulls)),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftLogical<Option<bool>> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => {
                // keep false values, all true values become nulls
                let false_values = self.as_arrow()?.values().not();
                let nulls = if let Some(original_nulls) = self.nulls() {
                    false_values.bitand(original_nulls.inner())
                } else {
                    false_values
                };

                self.with_nulls(Some(nulls.into()))
            }
            Some(rhs) => self.and(rhs),
        }
    }

    fn or(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => {
                // keep true values, all false values become nulls
                let true_arr = self.as_arrow()?;
                let true_values = true_arr.values();
                let nulls = if let Some(original_nulls) = self.nulls() {
                    true_values.bitand(original_nulls.inner())
                } else {
                    true_values.clone()
                };

                self.with_nulls(Some(nulls.into()))
            }
            Some(rhs) => self.or(rhs),
        }
    }

    fn xor(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => self.with_nulls(Some(NullBuffer::new_null(self.len()))),
            Some(rhs) => self.xor(rhs),
        }
    }
}

impl DaftLogical<bool> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: bool) -> Self::Output {
        if rhs {
            Ok(self.clone())
        } else {
            Self::from_arrow(
                Field::new(self.name(), DataType::Boolean),
                Arc::new(arrow::array::BooleanArray::new(
                    BooleanBuffer::new_unset(self.len()),
                    None,
                )),
            )
        }
    }

    fn or(&self, rhs: bool) -> Self::Output {
        if rhs {
            Self::from_arrow(
                Field::new(self.name(), DataType::Boolean),
                Arc::new(arrow::array::BooleanArray::new(
                    BooleanBuffer::new_set(self.len()),
                    None,
                )),
            )
        } else {
            Ok(self.clone())
        }
    }

    fn xor(&self, rhs: bool) -> Self::Output {
        if rhs { self.not() } else { Ok(self.clone()) }
    }
}

/// Macro to implement scalar comparison for byte-like array types (Utf8Array, BinaryArray, FixedSizeBinaryArray)
macro_rules! impl_scalar_compare {
    ($array_type:ty, $scalar_type:ty, $arrow_scalar:ty) => {
        impl $array_type {
            fn compare_to_scalar(
                &self,
                rhs: $scalar_type,
                op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
            ) -> DaftResult<BooleanArray> {
                let rhs = <$arrow_scalar>::new_scalar(rhs);
                let arrow_arr = op(&self.to_arrow(), &rhs)?;

                BooleanArray::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow_arr),
                )
            }
        }

        impl DaftCompare<$scalar_type> for $array_type {
            type Output = DaftResult<BooleanArray>;

            fn equal(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::eq)
            }

            fn not_equal(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::neq)
            }

            fn lt(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::lt)
            }

            fn lte(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::lt_eq)
            }

            fn gt(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::gt)
            }

            fn gte(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::gt_eq)
            }

            fn eq_null_safe(&self, rhs: $scalar_type) -> Self::Output {
                let eq_arr = self.compare_to_scalar(rhs, cmp::eq)?;

                Ok(if let Some(nulls) = eq_arr.nulls() {
                    let valid_arr = BooleanArray::from_null_buffer("", nulls)?;
                    eq_arr.with_nulls(None)?.and(&valid_arr)?
                } else {
                    eq_arr
                })
            }
        }
    };
}

impl_scalar_compare!(Utf8Array, &str, arrow::array::LargeStringArray);
impl_scalar_compare!(BinaryArray, &[u8], arrow::array::LargeBinaryArray);
impl_scalar_compare!(
    FixedSizeBinaryArray,
    &[u8],
    arrow::array::FixedSizeBinaryArray
);

#[cfg(test)]
mod tests {
    use common_error::{DaftError, DaftResult};
    use daft_arrow::buffer::NullBuffer;
    use rstest::rstest;

    use crate::{
        array::{
            ListArray, StructArray,
            ops::{DaftCompare, DaftLogical, full::FullNull},
        },
        datatypes::{
            BinaryArray, BooleanArray, DataType, Field, FixedSizeBinaryArray, Int64Array,
            NullArray, Utf8Array,
        },
        series::IntoSeries,
    };

    #[test]
    fn equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(true)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.lt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.lte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.gt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.gte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(true)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn compare_list_arrays_with_nulls() -> DaftResult<()> {
        let values = Int64Array::from_slice("item", &[1, 2, 3]).into_series();
        let left_rows = vec![None, Some(values.clone())];
        let right_rows = vec![None, Some(values)];
        let left = ListArray::from_series("left", left_rows)?;
        let right = ListArray::from_series("right", right_rows)?;

        let eq: Vec<_> = left.equal(&right)?.into_iter().collect();
        let eq_null_safe: Vec<_> = left.eq_null_safe(&right)?.into_iter().collect();

        assert_eq!(eq[..], [None, Some(true)]);
        assert_eq!(eq_null_safe[..], [Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn compare_struct_arrays_null_safe() -> DaftResult<()> {
        let field = Field::new(
            "struct",
            DataType::Struct(vec![
                Field::new("x", DataType::Int64),
                Field::new("y", DataType::Utf8),
            ]),
        );
        let nulls = NullBuffer::from_iter([true, false]);

        let left = StructArray::new(
            field.clone(),
            vec![
                Int64Array::from_slice("x", &[1, 2]).into_series(),
                Utf8Array::from_slice("y", &["a", "b"][..]).into_series(),
            ],
            Some(nulls.clone()),
        );
        let right = StructArray::new(
            field,
            vec![
                Int64Array::from_slice("x", &[1, 2]).into_series(),
                Utf8Array::from_slice("y", &["a", "b"][..]).into_series(),
            ],
            Some(nulls),
        );

        let eq: Vec<_> = left.equal(&right)?.into_iter().collect();
        let eq_null_safe: Vec<_> = left.eq_null_safe(&right)?.into_iter().collect();

        assert_eq!(eq[..], [Some(true), None]);
        assert_eq!(eq_null_safe[..], [Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn compare_nested_list() -> DaftResult<()> {
        let inner1 = Int64Array::from_slice("item", &[1, 2]).into_series();
        let inner2 = Int64Array::from_slice("item", &[3, 4]).into_series();

        let left_rows = vec![Some(
            ListArray::from_series("inner", vec![Some(inner1.clone())])?.into_series(),
        )];
        let right_rows = vec![Some(
            ListArray::from_series("inner", vec![Some(inner2)])?.into_series(),
        )];

        let left = ListArray::from_series("left", left_rows)?;
        let right = ListArray::from_series("right", right_rows)?;

        let lt: Vec<_> = left.lt(&right)?.into_iter().collect();
        let eq: Vec<_> = left.equal(&right)?.into_iter().collect();

        assert_eq!(lt[..], [Some(true)]);
        assert_eq!(eq[..], [Some(false)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(false)]);

        let array = array.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let lhs = lhs.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_nulls_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let lhs = lhs.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_nulls_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let lhs = lhs.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_nulls_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let lhs = lhs.with_nulls_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_nulls_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_handles_null_alignment() -> DaftResult<()> {
        let lhs = Int64Array::from_slice("lhs", &[1, 2, 3, 4]);
        let lhs = lhs.with_nulls_slice(&[true, false, true, false])?;
        let rhs = Int64Array::from_slice("rhs", &[1, 20, 30, 4]);
        let rhs = rhs.with_nulls_slice(&[true, true, false, false])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = Int64Array::from_slice("lhs", &[1, 2, 3]);
        let lhs = lhs.with_nulls_slice(&[true, false, true])?;
        let rhs = Int64Array::from_slice("rhs", &[0]);
        let rhs = rhs.with_nulls_slice(&[false])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_broadcast_null_lhs() -> DaftResult<()> {
        let lhs = Int64Array::from_slice("lhs", &[0]);
        let lhs = lhs.with_nulls_slice(&[false])?;
        let rhs = Int64Array::from_slice("rhs", &[1, 2, 3]);
        let rhs = rhs.with_nulls_slice(&[true, false, true])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_length_mismatch_errors() {
        let lhs = Int64Array::from_slice("lhs", &[1, 2, 3]);
        let rhs = Int64Array::from_slice("rhs", &[1, 2]);

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn eq_null_safe_boolean_handles_null_alignment() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(true), None, Some(false), None].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [Some(true), Some(false), None, None].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_boolean_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(true), Some(false), None].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [None].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(false), Some(true)]);
        Ok(())
    }

    #[test]
    fn boolean_and_handles_nulls() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(true), Some(false), None].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [Some(true), None, Some(true)].into_iter());

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), None]);
        Ok(())
    }

    #[test]
    fn boolean_or_handles_nulls() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(true), Some(false), None].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [Some(false), None, Some(true)].into_iter());

        let result: Vec<_> = lhs.or(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn boolean_and_with_null_scalar() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(false), Some(true)].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [None].into_iter());

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), None]);
        Ok(())
    }

    #[test]
    fn boolean_or_with_null_scalar() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [Some(true), Some(false)].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [None].into_iter());

        let result: Vec<_> = lhs.or(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), None]);
        Ok(())
    }

    #[test]
    fn boolean_and_null_lhs_broadcasts() -> DaftResult<()> {
        let lhs = BooleanArray::from_iter("lhs", [None::<bool>].into_iter());
        let rhs = BooleanArray::from_iter("rhs", [Some(false), Some(true)].into_iter());

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), None]);
        Ok(())
    }

    #[test]
    fn null_array_equal_returns_nulls() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 2);

        let eq: Vec<_> = lhs.equal(&rhs)?.into_iter().collect();
        assert_eq!(eq, vec![None, None]);
        Ok(())
    }

    #[test]
    fn null_array_eq_null_safe() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 2);

        let eq_null_safe: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(eq_null_safe, vec![Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn null_array_equal_broadcasts() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 3);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 1);

        let result: Vec<_> = lhs.equal(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![None, None, None]);
        Ok(())
    }

    #[test]
    fn null_array_eq_null_safe_broadcasts() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 3);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 1);

        let eq_null_safe: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(eq_null_safe, vec![Some(true), Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn null_array_equal_length_mismatch_errors() {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 3);

        let err = lhs.equal(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn null_array_eq_null_safe_length_mismatch_errors() {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 3);

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn utf8_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = Utf8Array::from_iter("lhs", vec![Some("a"), None, Some("c"), None].into_iter());
        let rhs = Utf8Array::from_iter("rhs", vec![Some("a"), Some("b"), None, None].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn utf8_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = Utf8Array::from_iter("vals", vec![Some("a"), None, Some("b")].into_iter());

        let result: Vec<_> = array.eq_null_safe("a")?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = BinaryArray::from_iter(
            "lhs",
            vec![Some(&b"aa"[..]), None, Some(&b"cc"[..]), None].into_iter(),
        );
        let rhs = BinaryArray::from_iter(
            "rhs",
            vec![Some(&b"aa"[..]), Some(&b"bb"[..]), None, None].into_iter(),
        );

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = BinaryArray::from_iter(
            "vals",
            vec![Some(&b"aa"[..]), None, Some(&b"bb"[..])].into_iter(),
        );

        let result: Vec<_> = array.eq_null_safe(&b"aa"[..])?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = BinaryArray::from_iter(
            "lhs",
            vec![Some(&b"aa"[..]), None, Some(&b"cc"[..])].into_iter(),
        );
        let rhs = BinaryArray::from_iter("rhs", vec![None::<&[u8]>].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), None, Some([3u8, 3u8]), None].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8]), None, None].into_iter(),
            2,
        );

        let result: Vec<_> = lhs.eq_null_safe(&rhs).unwrap().into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = FixedSizeBinaryArray::from_iter(
            "vals",
            vec![Some([1u8, 1u8]), None, Some([2u8, 2u8])].into_iter(),
            2,
        );

        let result: Vec<_> = array.eq_null_safe(&[1u8, 1u8][..])?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), None, Some([3u8, 3u8])].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter("rhs", vec![None::<&[u8]>].into_iter(), 2);

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_equal_broadcast_renames_to_lhs() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter("lhs", vec![Some([1u8, 1u8])].into_iter(), 2);
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8])].into_iter(),
            2,
        );

        let result = lhs.equal(&rhs)?;
        assert_eq!(result.name(), "lhs");
        let collected: Vec<_> = result.into_iter().collect();
        assert_eq!(collected, vec![Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_length_mismatch_errors() {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8])].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8]), Some([3u8, 3u8])].into_iter(),
            2,
        );

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[rstest]
    #[case::less_than(
        vec![vec![1, 2, 3]],
        vec![vec![1, 2, 4]],
        |l: &ListArray, r: &ListArray| l.lt(r),
        vec![Some(true)]
    )]
    #[case::less_than_by_length(
        vec![vec![1]],
        vec![vec![1, 0]],
        |l: &ListArray, r: &ListArray| l.lt(r),
        vec![Some(true)]
    )]
    #[case::less_than_equal(
        vec![vec![1, 2, 3]],
        vec![vec![1, 2, 3]],
        |l: &ListArray, r: &ListArray| l.lte(r),
        vec![Some(true)]
    )]
    #[case::greater_than(
        vec![vec![1, 2, 4]],
        vec![vec![1, 2, 3]],
        |l: &ListArray, r: &ListArray| l.gt(r),
        vec![Some(true)]
    )]
    #[case::greater_than_equal(
        vec![vec![5]],
        vec![vec![5]],
        |l: &ListArray, r: &ListArray| l.gte(r),
        vec![Some(true)]
    )]
    #[case::equal(
        vec![vec![1, 2, 3]],
        vec![vec![1, 2, 3]],
        |l: &ListArray, r: &ListArray| l.equal(r),
        vec![Some(true)]
    )]
    #[case::not_equal(
        vec![vec![1, 2, 3]],
        vec![vec![1, 2, 4]],
        |l: &ListArray, r: &ListArray| l.not_equal(r),
        vec![Some(true)]
    )]
    // Multi-element test cases
    #[case::multi_less_than(
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        vec![vec![1, 2, 4], vec![5, 6, 7]],
        |l: &ListArray, r: &ListArray| l.lt(r),
        vec![Some(true), Some(true)]
    )]
    #[case::multi_greater_than(
        vec![vec![1, 2, 4], vec![5, 6, 7]],
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        |l: &ListArray, r: &ListArray| l.gt(r),
        vec![Some(true), Some(true)]
    )]
    #[case::multi_equal(
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        |l: &ListArray, r: &ListArray| l.equal(r),
        vec![Some(true), Some(true)]
    )]
    fn compare_list_parametrized(
        #[case] left_vals: Vec<Vec<i64>>,
        #[case] right_vals: Vec<Vec<i64>>,
        #[case] op: impl Fn(&ListArray, &ListArray) -> DaftResult<BooleanArray>,
        #[case] expected: Vec<Option<bool>>,
    ) -> DaftResult<()> {
        let left_rows: Vec<_> = left_vals
            .into_iter()
            .map(|v| Some(Int64Array::from_vec("item", v).into_series()))
            .collect();
        let right_rows: Vec<_> = right_vals
            .into_iter()
            .map(|v| Some(Int64Array::from_vec("item", v).into_series()))
            .collect();

        let left = ListArray::from_series("left", left_rows)?;
        let right = ListArray::from_series("right", right_rows)?;

        let result: Vec<_> = op(&left, &right)?.into_iter().collect();
        assert_eq!(result[..], expected[..]);
        Ok(())
    }

    #[rstest]
    #[case::less_than(
        vec![(1, "a")],
        vec![(1, "b")],
        |l: &StructArray, r: &StructArray| l.lt(r),
        vec![Some(true)]
    )]
    #[case::less_than_equal(
        vec![(2, "b")],
        vec![(2, "b")],
        |l: &StructArray, r: &StructArray| l.lte(r),
        vec![Some(true)]
    )]
    #[case::greater_than(
        vec![(3, "z")],
        vec![(3, "a")],
        |l: &StructArray, r: &StructArray| l.gt(r),
        vec![Some(true)]
    )]
    #[case::greater_than_equal(
        vec![(1, "a")],
        vec![(1, "a")],
        |l: &StructArray, r: &StructArray| l.gte(r),
        vec![Some(true)]
    )]
    #[case::equal(
        vec![(5, "test")],
        vec![(5, "test")],
        |l: &StructArray, r: &StructArray| l.equal(r),
        vec![Some(true)]
    )]
    #[case::not_equal(
        vec![(1, "a")],
        vec![(1, "b")],
        |l: &StructArray, r: &StructArray| l.not_equal(r),
        vec![Some(true)]
    )]
    // Multi-element test cases
    #[case::multi_less_than(
        vec![(1, "a"), (2, "b")],
        vec![(1, "b"), (2, "c")],
        |l: &StructArray, r: &StructArray| l.lt(r),
        vec![Some(true), Some(true)]
    )]
    #[case::multi_equal_mixed(
        vec![(1, "a"), (2, "b")],
        vec![(1, "a"), (2, "c")],
        |l: &StructArray, r: &StructArray| l.equal(r),
        vec![Some(true), Some(false)]
    )]
    fn compare_struct_parametrized(
        #[case] left_vals: Vec<(i64, &str)>,
        #[case] right_vals: Vec<(i64, &str)>,
        #[case] op: impl Fn(&StructArray, &StructArray) -> DaftResult<BooleanArray>,
        #[case] expected: Vec<Option<bool>>,
    ) -> DaftResult<()> {
        let field = Field::new(
            "struct",
            DataType::Struct(vec![
                Field::new("x", DataType::Int64),
                Field::new("y", DataType::Utf8),
            ]),
        );

        let left_x: Vec<_> = left_vals.iter().map(|(x, _)| *x).collect();
        let left_y: Vec<_> = left_vals.iter().map(|(_, y)| *y).collect();
        let right_x: Vec<_> = right_vals.iter().map(|(x, _)| *x).collect();
        let right_y: Vec<_> = right_vals.iter().map(|(_, y)| *y).collect();

        let left = StructArray::new(
            field.clone(),
            vec![
                Int64Array::from_vec("x", left_x).into_series(),
                Utf8Array::from_slice("y", left_y.as_slice()).into_series(),
            ],
            None,
        );
        let right = StructArray::new(
            field,
            vec![
                Int64Array::from_vec("x", right_x).into_series(),
                Utf8Array::from_slice("y", right_y.as_slice()).into_series(),
            ],
            None,
        );

        let result: Vec<_> = op(&left, &right)?.into_iter().collect();
        assert_eq!(result[..], expected[..]);
        Ok(())
    }
}
