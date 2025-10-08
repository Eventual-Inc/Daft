mod tests {
    use common_error::DaftError;
    use daft_core::prelude::*;
    use rstest::rstest;

    use crate::{
        ExprRef,
        functions::{FunctionArg, FunctionArgs},
        lit,
    };

    #[derive(FunctionArgs)]
    struct BasicArgs<T> {
        arg1: T,
        arg2: T,
    }

    #[derive(FunctionArgs)]
    struct OptionalArgs<T> {
        arg1: T,
        #[arg(optional)]
        arg2: Option<T>,
        #[arg(optional)]
        arg3: Option<T>,
    }

    #[derive(FunctionArgs)]
    struct VariadicArgs<T> {
        arg1: T,
        #[arg(variadic)]
        arg2: Vec<T>,
    }

    #[derive(FunctionArgs)]
    struct OptionalAndVariadicArgs<T> {
        arg1: T,
        #[arg(variadic)]
        arg2: Vec<T>,
        #[arg(optional)]
        arg3: Option<T>,
    }

    #[derive(FunctionArgs)]
    struct LiteralArgs<T> {
        arg1: T,
        arg2: usize,
    }

    #[derive(FunctionArgs)]
    struct OnlyLiteralArgs {
        arg1: usize,
    }

    #[derive(FunctionArgs)]
    struct OptionalLiteralArgs {
        #[arg(optional)]
        arg1: Option<usize>,
    }

    #[derive(FunctionArgs)]
    struct VariadicLiteralArgs {
        #[arg(variadic)]
        arg1: Vec<usize>,
    }

    #[derive(FunctionArgs)]
    struct RenamedArgs<T> {
        #[arg(name = "arg2")]
        arg1: T,
        #[arg(name = "arg1")]
        arg2: T,
    }

    #[rstest]
    #[case(vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))])]
    #[case(vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg2", lit(2))])]
    #[case(vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg2", lit(2))])]
    #[case(vec![FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg1", lit(1))])]
    fn test_basic_valid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = BasicArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, lit(1));
        assert_eq!(parsed_args.arg2, lit(2));
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![FunctionArg::unnamed(lit(1))])]
    #[case(vec![
        FunctionArg::unnamed(lit(1)),
        FunctionArg::unnamed(lit(2)),
        FunctionArg::unnamed(lit(3)),
    ])]
    #[case(vec![FunctionArg::named("arg1", lit(1))])]
    #[case(vec![FunctionArg::named("arg1", lit(1)), FunctionArg::unnamed(lit(2))])]
    #[case(vec![FunctionArg::named("arg3", lit(3))])]
    #[case(vec![
        FunctionArg::named("arg1", lit(1)),
        FunctionArg::named("arg2", lit(2)),
        FunctionArg::named("arg3", lit(3)),
    ])]
    fn test_basic_invalid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let parse_args = || {
            let function_args = FunctionArgs::try_new(args)?;
            BasicArgs::try_from(function_args)?;

            Ok::<_, DaftError>(())
        };

        parse_args().expect_err("should fail");
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2)), FunctionArg::unnamed(lit(3))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))],
        (lit(1), Some(lit(2)), None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1))],
        (lit(1), None, None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg2", lit(2))],
        (lit(1), Some(lit(2)), None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg3", lit(3))],
        (lit(1), None, Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg3", lit(3))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg3", lit(3)), FunctionArg::named("arg2", lit(2))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg2", lit(2))],
        (lit(1), Some(lit(2)), None)
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg3", lit(3))],
        (lit(1), None, Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg3", lit(3))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg3", lit(3)), FunctionArg::named("arg2", lit(2))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg3", lit(3))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg3", lit(3)), FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg2", lit(2))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg3", lit(3)), FunctionArg::named("arg1", lit(1))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::named("arg3", lit(3)), FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg1", lit(1))],
        (lit(1), Some(lit(2)), Some(lit(3)))
    )]
    fn test_optional_valid(
        #[case] args: Vec<FunctionArg<ExprRef>>,
        #[case] expected: (ExprRef, Option<ExprRef>, Option<ExprRef>),
    ) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = OptionalArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, expected.0);
        assert_eq!(parsed_args.arg2, expected.1);
        assert_eq!(parsed_args.arg3, expected.2);
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![FunctionArg::named("arg2", lit(2))])]
    #[case(vec![FunctionArg::named("arg3", lit(3))])]
    #[case(vec![FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg3", lit(3))])]
    fn test_optional_invalid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let parse_args = || {
            let function_args = FunctionArgs::try_new(args)?;
            OptionalArgs::try_from(function_args)?;

            Ok::<_, DaftError>(())
        };

        parse_args().expect_err("should fail");
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1))],
        (lit(1), vec![])
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))],
        (lit(1), vec![lit(2)])
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2)), FunctionArg::unnamed(lit(3))],
        (lit(1), vec![lit(2), lit(3)])
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1))],
        (lit(1), vec![])
    )]
    fn test_variadic_valid(
        #[case] args: Vec<FunctionArg<ExprRef>>,
        #[case] expected: (ExprRef, Vec<ExprRef>),
    ) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = VariadicArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, expected.0);
        assert_eq!(parsed_args.arg2, expected.1);
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![FunctionArg::named("arg2", lit(2))])]
    #[case(vec![FunctionArg::unnamed(lit(2)), FunctionArg::named("arg1", lit(1))])]
    fn test_variadic_invalid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let parse_args = || {
            let function_args = FunctionArgs::try_new(args)?;
            VariadicArgs::try_from(function_args)?;

            Ok::<_, DaftError>(())
        };

        parse_args().expect_err("should fail");
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1))],
        (lit(1), vec![], None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))],
        (lit(1), vec![lit(2)], None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2)), FunctionArg::unnamed(lit(3))],
        (lit(1), vec![lit(2), lit(3)], None)
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg3", lit(3))],
        (lit(1), vec![], Some(lit(3)))
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2)), FunctionArg::named("arg3", lit(3))],
        (lit(1), vec![lit(2)], Some(lit(3)))
    )]
    fn test_optional_and_variadic_valid(
        #[case] args: Vec<FunctionArg<ExprRef>>,
        #[case] expected: (ExprRef, Vec<ExprRef>, Option<ExprRef>),
    ) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = OptionalAndVariadicArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, expected.0);
        assert_eq!(parsed_args.arg2, expected.1);
        assert_eq!(parsed_args.arg3, expected.2);
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))],
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg2", lit(2))],
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1)), FunctionArg::named("arg2", lit(2))],
    )]
    #[case(
        vec![FunctionArg::named("arg2", lit(2)), FunctionArg::named("arg1", lit(1))],
    )]
    fn test_literal_valid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = LiteralArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, lit(1));
        assert_eq!(parsed_args.arg2, 2);
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(Literal::from(1).into()), FunctionArg::unnamed(Literal::from(2).into())],
    )]
    #[case(
        vec![FunctionArg::unnamed(Literal::from(1).into()), FunctionArg::named("arg2", Literal::from(2).into())],
    )]
    #[case(
        vec![FunctionArg::named("arg1", Literal::from(1).into()), FunctionArg::named("arg2", Literal::from(2).into())],
    )]
    #[case(
        vec![FunctionArg::named("arg2", Literal::from(2).into()), FunctionArg::named("arg1", Literal::from(1).into())],
    )]
    fn test_literal_series_valid(#[case] args: Vec<FunctionArg<Series>>) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = LiteralArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, Literal::from(1).into());
        assert_eq!(parsed_args.arg2, 2);
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(-2))],
    )]
    fn test_literal_invalid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let parse_args = || {
            let function_args = FunctionArgs::try_new(args)?;
            LiteralArgs::try_from(function_args)?;

            Ok::<_, DaftError>(())
        };

        parse_args().expect_err("should fail");
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1))],
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1))],
    )]
    fn test_only_literal_valid(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = OnlyLiteralArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, 1);
    }

    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1))], Some(1)
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(1))], Some(1)
    )]
    #[case(
        vec![], None
    )]
    fn test_optional_literal_valid(
        #[case] args: Vec<FunctionArg<ExprRef>>,
        #[case] expected: Option<usize>,
    ) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = OptionalLiteralArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, expected);
    }

    #[rstest]
    #[case(
        vec![], vec![]
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1))], vec![1]
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))], vec![1, 2]
    )]
    fn test_variadic_literal_valid(
        #[case] args: Vec<FunctionArg<ExprRef>>,
        #[case] expected: Vec<usize>,
    ) {
        let function_args = FunctionArgs::try_new(args).unwrap();

        let parsed_args = VariadicLiteralArgs::try_from(function_args).expect("should succeed");
        assert_eq!(parsed_args.arg1, expected);
    }
    #[rstest]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::unnamed(lit(2))],
    )]
    #[case(
        vec![FunctionArg::unnamed(lit(1)), FunctionArg::named("arg1", lit(2))],
    )]
    #[case(
        vec![FunctionArg::named("arg2", lit(1)), FunctionArg::named("arg1", lit(2))],
    )]
    #[case(
        vec![FunctionArg::named("arg1", lit(2)), FunctionArg::named("arg2", lit(1))],
    )]
    fn test_renamed(#[case] args: Vec<FunctionArg<ExprRef>>) {
        let function_args = FunctionArgs::try_new(args).unwrap();
        let parsed_args = RenamedArgs::try_from(function_args).expect("should succeed");

        assert_eq!(parsed_args.arg1, lit(1));
        assert_eq!(parsed_args.arg2, lit(2));
    }
}
