use super::*;
use crate::col;

#[test]
fn test_get_common_join_keys() {
    let left_on: &[ExprRef] = &[
        col("a"),
        col("b_left"),
        col("c").alias("c_new"),
        col("d").alias("d_new"),
        col("e").add(col("f")),
    ];

    let right_on: &[ExprRef] = &[
        col("a"),
        col("b_right"),
        col("c"),
        col("d").alias("d_new"),
        col("e"),
    ];

    let common_join_keys = get_common_join_keys(left_on, right_on)
        .map(|k| k.to_string())
        .collect::<Vec<_>>();

    assert_eq!(common_join_keys, vec!["a"]);
}
