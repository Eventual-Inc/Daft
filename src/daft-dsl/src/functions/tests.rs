#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use serde::{Deserialize, Serialize};

    use crate::{
        functions::{ScalarFunction, ScalarUDF, SpecialFunction},
        ExprRef,
    };

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    struct TestSpecialFunction {}

    impl SpecialFunction for TestSpecialFunction {}

    #[typetag::serde]
    impl ScalarUDF for TestSpecialFunction {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &'static str {
            "test_special_function"
        }

        fn evaluate(&self, _inputs: &[Series]) -> DaftResult<Series> {
            unimplemented!()
        }

        fn to_field(&self, _inputs: &[ExprRef], _schema: &Schema) -> DaftResult<Field> {
            unimplemented!()
        }

        fn is_special(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    struct TestRegularFunction {}

    #[typetag::serde]
    impl ScalarUDF for TestRegularFunction {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &'static str {
            "test_regular_function"
        }

        fn evaluate(&self, _inputs: &[Series]) -> DaftResult<Series> {
            unimplemented!()
        }

        fn to_field(&self, _inputs: &[ExprRef], _schema: &Schema) -> DaftResult<Field> {
            unimplemented!()
        }

        fn is_special(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_special_function_detection() {
        let special_fn = ScalarFunction::new(TestSpecialFunction {}, vec![]);
        let regular_fn = ScalarFunction::new(TestRegularFunction {}, vec![]);

        assert!(
            special_fn.is_special_function(),
            "TestSpecialFunction should be detected as a special function"
        );
        assert!(
            !regular_fn.is_special_function(),
            "TestRegularFunction should not be detected as a special function"
        );
    }
}
