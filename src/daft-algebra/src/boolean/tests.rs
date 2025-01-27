#[cfg(test)]
mod tests {
    use daft_dsl::col;

    use crate::boolean::{to_cnf, to_dnf};

    #[test]
    fn dnf_simple() {
        // a & (b | c) -> (a & b) | (a & c)
        let expr = col("a").and(col("b").or(col("c")));
        let expected = col("a").and(col("b")).or(col("a").and(col("c")));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_simple() {
        // a | (b & c) -> (a | b) & (a | c)
        let expr = col("a").or(col("b").and(col("c")));
        let expected = col("a").or(col("b")).and(col("a").or(col("c")));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_neg() {
        // !(a & ((!b) | c)) -> (!a) | (b & (!c))
        let expr = col("a").and(col("b").not().or(col("c"))).not();
        let expected = col("a").not().or(col("b").and(col("c").not()));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_neg() {
        // !(a | ((!b) & c)) -> (!a) & (b | (!c))
        let expr = col("a").or(col("b").not().and(col("c"))).not();
        let expected = col("a").not().and(col("b").or(col("c").not()));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_nested() {
        // a & b & ((c & d) | (e & f)) -> (a & b & c & d) | (a & b & e & f)
        let expr = col("a")
            .and(col("b"))
            .and((col("c").and(col("d"))).or(col("e").and(col("f"))));
        let expected = (col("a").and(col("b")).and(col("c").and(col("d"))))
            .or(col("a").and(col("b")).and(col("e").and(col("f"))));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_nested() {
        // a & b & ((c & d) | (e & f)) -> a & b & (c | e) & (c | f) & (d | e) & (d | f)
        let expr = col("a")
            .and(col("b"))
            .and((col("c").and(col("d"))).or(col("e").and(col("f"))));
        let expected = col("a").and(col("b")).and(
            (col("c").or(col("e")))
                .and(col("c").or(col("f")))
                .and(col("d").or(col("e")).and(col("d").or(col("f")))),
        );

        assert_eq!(expected, to_cnf(expr));
    }
}
