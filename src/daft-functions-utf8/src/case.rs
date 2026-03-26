use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase, ToTitleCase,
    ToUpperCamelCase,
};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, unary_utf8_evaluate, unary_utf8_to_field};

macro_rules! define_case_udf {
    ($struct:ident, $fn_name:ident, $method:ident, $docstring:literal) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $struct;

        #[typetag::serde]
        impl ScalarUDF for $struct {
            fn name(&self) -> &'static str {
                stringify!($fn_name)
            }

            fn call(
                &self,
                inputs: FunctionArgs<Series>,
                _ctx: &daft_dsl::functions::scalar::EvalContext,
            ) -> DaftResult<Series> {
                unary_utf8_evaluate(inputs, |s| {
                    s.with_utf8_array(|arr| {
                        Ok(arr
                            .unary_broadcasted_op(|val| val.$method().into())?
                            .into_series())
                    })
                })
            }

            fn get_return_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
            }

            fn docstring(&self) -> &'static str {
                $docstring
            }
        }

        #[must_use]
        pub fn $fn_name(input: ExprRef) -> ExprRef {
            ScalarFn::builtin($struct, vec![input]).into()
        }
    };
}

define_case_udf!(
    CamelCase,
    to_camel_case,
    to_lower_camel_case,
    "Converts a string to lower camel case."
);
define_case_udf!(
    UpperCamelCase,
    to_upper_camel_case,
    to_upper_camel_case,
    "Converts a string to upper camel case."
);
define_case_udf!(
    SnakeCase,
    to_snake_case,
    to_snake_case,
    "Converts a string to snake case."
);
define_case_udf!(
    UpperSnakeCase,
    to_upper_snake_case,
    to_shouty_snake_case,
    "Converts a string to upper snake case."
);
define_case_udf!(
    KebabCase,
    to_kebab_case,
    to_kebab_case,
    "Converts a string to kebab case."
);
define_case_udf!(
    UpperKebabCase,
    to_upper_kebab_case,
    to_shouty_kebab_case,
    "Converts a string to upper kebab case."
);
define_case_udf!(
    TitleCase,
    to_title_case,
    to_title_case,
    "Converts a string to title case."
);
