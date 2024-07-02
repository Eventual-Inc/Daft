use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, FieldID},
    schema::Schema,
    DataType, Series,
};
use serde::{Deserialize, Serialize};

use crate::{lit, Expr, ExprRef};

use super::{FunctionExpr, ScalarUDF};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinHashExpr;

impl MinHashExpr {
    pub const NAME: &'static str = "minhash";
}

pub fn minhash(input: ExprRef, num_hashes: ExprRef, ngram_size: ExprRef, seed: ExprRef) -> ExprRef {
    let e = MinHashExpr;

    Expr::Function {
        func: super::ScalarFunction::new(e, vec![num_hashes, ngram_size, seed]).into(),
        inputs: vec![input],
    }
    .into()
}

impl ScalarUDF for MinHashExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn semantic_id(&self) -> daft_core::datatypes::FieldID {
        FieldID::new(format!("MinHash"))
    }

    fn name(&self) -> &'static str {
        "minhash"
    }

    fn evaluate(&self, inputs: &[Series], args: &[ExprRef]) -> DaftResult<Series> {
        // let [num_hashes, ngram_size, seed] = args;

        match inputs {
            [input] => {
                // input.minhash(self.num_hashes, self.ngram_size, self.seed)
                todo!()
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        todo!()
        // match inputs {
        //     [data] => match data.to_field(schema) {
        //         Ok(data_field) => match &data_field.dtype {
        //             DataType::Utf8 => Ok(Field::new(
        //                 data_field.name,
        //                 DataType::FixedSizeList(Box::new(DataType::UInt32), self.num_hashes),
        //             )),
        //             _ => Err(DaftError::TypeError(format!(
        //                 "Expects input to minhash to be utf8, but received {data_field}",
        //             ))),
        //         },
        //         Err(e) => Err(e),
        //     },
        //     _ => Err(DaftError::SchemaMismatch(format!(
        //         "Expected 1 input args, got {}",
        //         inputs.len()
        //     ))),
        // }
    }
}
