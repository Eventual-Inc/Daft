use daft_dsl::ExprRef;

#[must_use]
pub fn fill_nan(input: ExprRef, fill_value: ExprRef) -> ExprRef {
    let predicate = super::not_nan(input.clone());
    predicate.if_else(input, fill_value)
}
