use std::{any::Any, marker::PhantomData};

use crate::datatypes::{DaftDataType, DataType};

pub trait BaseArray: Any {
    fn data(&self) -> &Box<dyn arrow2::array::Array>;

    fn data_type(&self) -> DataType;

    fn as_any(&self) -> &dyn std::any::Any;
}

impl std::fmt::Debug for dyn BaseArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.data_type())
    }
}

pub struct DataArray<T: DaftDataType> {
    data: Box<dyn arrow2::array::Array>,
    phantom: PhantomData<T>,
}

impl<T: DaftDataType> From<Box<dyn arrow2::array::Array>> for DataArray<T> {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        DataArray {
            data: item,
            phantom: PhantomData,
        }
    }
}

impl<T: DaftDataType + 'static> BaseArray for DataArray<T> {
    fn data(&self) -> &Box<dyn arrow2::array::Array> {
        &self.data
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataType {
        return self.data.data_type().into();
    }

    // fn binary_op(&self, other: &dyn BaseArray, op: Operator) -> DaftResult<Arc<dyn BaseArray>> {
    //     let mut lhs = self.data();
    //     let mut rhs = other.data();
    //     use arrow2::compute::arithmetics::*;

    //     use arrow2::compute::boolean::*;
    //     use arrow2::compute::comparison::*;

    //     if lhs.len() != rhs.len() {
    //         return Err(DaftError::ComputeError(format!(
    //             "lhs and rhs length do not match: {} vs {}",
    //             lhs.len(),
    //             rhs.len()
    //         )));
    //     }

    //     use crate::dsl::Operator::*;

    //     let stype = match supertype::get_supertype(&lhs.data_type().into(), &rhs.data_type().into())
    //     {
    //         Some(val) => Some(val.to_arrow()?),
    //         None => None,
    //     };

    //     let can_run = |ltype, rtype| match op {
    //         Eq => ltype == rtype && can_eq(ltype),
    //         NotEq => ltype == rtype && can_neq(ltype),
    //         Lt => ltype == rtype && can_lt(ltype),
    //         LtEq => ltype == rtype && can_lt_eq(ltype),
    //         Gt => ltype == rtype && can_gt(ltype),
    //         GtEq => ltype == rtype && can_gt_eq(ltype),
    //         Plus => {
    //             can_add(ltype, rtype)
    //                 || (ltype.eq(rtype) && ltype.eq(&arrow2::datatypes::DataType::LargeUtf8))
    //         }
    //         Minus => can_sub(ltype, rtype),
    //         Multiply => can_mul(ltype, rtype),
    //         Divide => can_div(ltype, rtype),
    //         TrueDivide => can_div(ltype, rtype),
    //         FloorDivide => can_div(ltype, rtype), // TODO(floor this)
    //         Modulus => can_rem(ltype, rtype),
    //         And => {
    //             ltype.eq(&arrow2::datatypes::DataType::Boolean)
    //                 && rtype.eq(&arrow2::datatypes::DataType::Boolean)
    //         }
    //         Or => {
    //             ltype.eq(&arrow2::datatypes::DataType::Boolean)
    //                 && rtype.eq(&arrow2::datatypes::DataType::Boolean)
    //         }
    //         Xor => {
    //             ltype.eq(&arrow2::datatypes::DataType::Boolean)
    //                 && rtype.eq(&arrow2::datatypes::DataType::Boolean)
    //         }
    //     };
    //     use arrow2::compute::cast::*;

    //     let can_run_without_cast = can_run(lhs.data_type(), rhs.data_type());

    //     if !can_run_without_cast && stype.is_none() {
    //         return Err(DaftError::ComputeError(format!(
    //             "op: {:?}: lhs and rhs have incompatible types: {:?} vs {:?} with no supertype",
    //             op,
    //             lhs.data_type(),
    //             rhs.data_type()
    //         )));
    //     }
    //     let stype = stype.unwrap();
    //     let mut can_run_with_cast = false;

    //     // Determines if we can cast lhs and rhs to supertypes if we can't run the native kernel
    //     let casted_lhs;
    //     let casted_rhs;
    //     if !can_run_without_cast
    //         && can_run(&stype, &stype)
    //         && can_cast_types(lhs.data_type(), &stype)
    //         && can_cast_types(rhs.data_type(), &stype)
    //     {
    //         can_run_with_cast = true;
    //         casted_lhs = cast(
    //             lhs.as_ref(),
    //             &stype,
    //             CastOptions {
    //                 wrapped: true,
    //                 partial: false,
    //             },
    //         )?;
    //         casted_rhs = cast(
    //             rhs.as_ref(),
    //             &stype,
    //             CastOptions {
    //                 wrapped: true,
    //                 partial: false,
    //             },
    //         )?;
    //         lhs = &casted_lhs;
    //         rhs = &casted_rhs;
    //     }

    //     if !can_run_without_cast && !can_run_with_cast {
    //         println!(
    //             "{:?} {:?} {:?}",
    //             can_run(&stype, &stype),
    //             can_cast_types(lhs.data_type(), &stype),
    //             can_cast_types(rhs.data_type(), &stype)
    //         );
    //         return Err(DaftError::ComputeError(format!(
    //             "op: {:?}: lhs and rhs have incompatible types: {:?} vs {:?}. SuperType: {:?}",
    //             op,
    //             lhs.data_type(),
    //             rhs.data_type(),
    //             stype
    //         )));
    //     }

    //     let lhs = lhs.as_ref();
    //     let rhs = rhs.as_ref();

    //     let result_array = match op {
    //         Eq => Box::from(eq(lhs, rhs)),
    //         NotEq => Box::from(neq(lhs, rhs)),
    //         Lt => Box::from(lt(lhs, rhs)),
    //         LtEq => Box::from(lt_eq(lhs, rhs)),
    //         Gt => Box::from(gt(lhs, rhs)),
    //         GtEq => Box::from(gt_eq(lhs, rhs)),
    //         Plus => {
    //             if lhs.data_type().eq(rhs.data_type())
    //                 && lhs.data_type().eq(&arrow2::datatypes::DataType::LargeUtf8)
    //             {
    //                 add_utf8_arrays(
    //                     lhs.as_any().downcast_ref().unwrap(),
    //                     rhs.as_any().downcast_ref().unwrap(),
    //                 )?
    //                 .boxed()
    //             } else {
    //                 add(lhs, rhs)
    //             }
    //         }
    //         Minus => sub(lhs, rhs),
    //         Multiply => mul(lhs, rhs),
    //         Divide => div(lhs, rhs),
    //         TrueDivide => div(lhs, rhs),
    //         FloorDivide => div(lhs, rhs), // TODO(floor this)
    //         Modulus => rem(lhs, rhs),
    //         And => Box::from(and(
    //             lhs.as_any().downcast_ref().unwrap(),
    //             rhs.as_any().downcast_ref().unwrap(),
    //         )),
    //         Or => Box::from(or(
    //             lhs.as_any().downcast_ref().unwrap(),
    //             rhs.as_any().downcast_ref().unwrap(),
    //         )),
    //         Xor => panic!("Xor Not supported"),
    //     };

    //     if !result_array.data_type().eq(&stype) {
    //         return Err(DaftError::TypeError(format!(
    //             "expected result arrow to be the same as computed supertype: {:?} vs {:?}",
    //             result_array.data_type(),
    //             stype
    //         )));
    //     }

    //     Ok(Arc::from(DataArray::<Int64Type> {
    //         data: result_array,
    //         phantom: PhantomData,
    //     }))
    // }
}
