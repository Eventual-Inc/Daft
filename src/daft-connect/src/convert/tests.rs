use std::collections::HashMap;
use crate::command::execute_plan;
use crate::convert::to_logical_plan;
use crate::spark_connect::{Expression, Filter, Read, Relation, RelationCommon, ShowString, WithColumns};
use crate::spark_connect::expression::{Alias, ExprType, Literal, UnresolvedAttribute, UnresolvedFunction};
use crate::spark_connect::expression::literal::LiteralType;
use crate::spark_connect::read::{DataSource, ReadType};
use crate::spark_connect::relation::RelType;

#[test]
pub fn test_filter() {
    #[expect(
        deprecated,
        reason = "Some of the fields are deprecated, but we must still set them to their default values."
    )]
    let input = Relation {
        common: Some(
            RelationCommon {
                source_info: String::new(),
                plan_id: Some(
                    4,
                ),
                origin: None,
            },
        ),
        rel_type: Some(
            RelType::ShowString(
                Box::new(ShowString {
                    input: Some(
                        Box::new(Relation {
                            common: Some(
                                RelationCommon {
                                    source_info: String::new(),
                                    plan_id: Some(
                                        3,
                                    ),
                                    origin: None,
                                },
                            ),
                            rel_type: Some(
                                RelType::WithColumns(
                                    Box::new(WithColumns {
                                        input: Some(
                                            Box::new(Relation {
                                                common: Some(
                                                    RelationCommon {
                                                        source_info: String::new(),
                                                        plan_id: Some(
                                                            2,
                                                        ),
                                                        origin: None,
                                                    },
                                                ),
                                                rel_type: Some(
                                                    RelType::Filter(
                                                        Box::new(Filter {
                                                            input: Some(
                                                                Box::new(Relation {
                                                                    common: Some(
                                                                        RelationCommon {
                                                                            source_info: String::new(),
                                                                            plan_id: Some(
                                                                                0,
                                                                            ),
                                                                            origin: None,
                                                                        },
                                                                    ),
                                                                    rel_type: Some(
                                                                        RelType::Read(
                                                                            Read {
                                                                                is_streaming: false,
                                                                                read_type: Some(
                                                                                    ReadType::DataSource(
                                                                                        DataSource {
                                                                                            format: Some(
                                                                                                "parquet".to_string(),
                                                                                            ),
                                                                                            schema: Some(
                                                                                                String::new(),
                                                                                            ),
                                                                                            options: HashMap::new(),
                                                                                            paths: vec![
                                                                                                "/Users/andrewgazelka/Projects/simple-spark-connect/increasing_id_data.parquet".to_string(),
                                                                                            ],
                                                                                            predicates: Vec::new(),
                                                                                        },
                                                                                    ),
                                                                                ),
                                                                            },
                                                                        ),
                                                                    ),
                                                                }),
                                                            ),
                                                            condition: Some(
                                                                Expression {
                                                                    common: None,
                                                                    expr_type: Some(
                                                                        ExprType::UnresolvedFunction(
                                                                            UnresolvedFunction {
                                                                                function_name: ">".to_string(),
                                                                                arguments: vec![
                                                                                    Expression {
                                                                                        common: None,
                                                                                        expr_type: Some(
                                                                                            ExprType::UnresolvedAttribute(
                                                                                                UnresolvedAttribute {
                                                                                                    unparsed_identifier: "id".to_string(),
                                                                                                    plan_id: None,
                                                                                                    is_metadata_column: None,
                                                                                                },
                                                                                            ),
                                                                                        ),
                                                                                    },
                                                                                    Expression {
                                                                                        common: None,
                                                                                        expr_type: Some(
                                                                                            ExprType::Literal(
                                                                                                Literal {
                                                                                                    literal_type: Some(
                                                                                                        LiteralType::Integer(
                                                                                                            2,
                                                                                                        ),
                                                                                                    ),
                                                                                                },
                                                                                            ),
                                                                                        ),
                                                                                    },
                                                                                ],
                                                                                is_distinct: false,
                                                                                is_user_defined_function: false,
                                                                            },
                                                                        ),
                                                                    ),
                                                                },
                                                            ),
                                                        }),
                                                    ),
                                                ),
                                            }),
                                        ),
                                        aliases: vec![
                                            Alias {
                                                expr: Some(
                                                    Box::new(Expression {
                                                        common: None,
                                                        expr_type: Some(
                                                            ExprType::UnresolvedFunction(
                                                                UnresolvedFunction {
                                                                    function_name: "+".to_string(),
                                                                    arguments: vec![
                                                                        Expression {
                                                                            common: None,
                                                                            expr_type: Some(
                                                                                ExprType::UnresolvedAttribute(
                                                                                    UnresolvedAttribute {
                                                                                        unparsed_identifier: "id".to_string(),
                                                                                        plan_id: None,
                                                                                        is_metadata_column: None,
                                                                                    },
                                                                                ),
                                                                            ),
                                                                        },
                                                                        Expression {
                                                                            common: None,
                                                                            expr_type: Some(
                                                                                ExprType::Literal(
                                                                                    Literal {
                                                                                        literal_type: Some(
                                                                                            LiteralType::Integer(
                                                                                                2,
                                                                                            ),
                                                                                        ),
                                                                                    },
                                                                                ),
                                                                            ),
                                                                        },
                                                                    ],
                                                                    is_distinct: false,
                                                                    is_user_defined_function: false,
                                                                },
                                                            ),
                                                        ),
                                                    }),
                                                ),
                                                name: vec![
                                                    "id2".to_string(),
                                                ],
                                                metadata: None,
                                            },
                                        ],
                                    }),
                                ),
                            ),
                        }),
                    ),
                    num_rows: 20,
                    truncate: 20,
                    vertical: false,
                }),
            ),
        ),
    };

    let plan = to_logical_plan(input).unwrap()
        .build();

    let result = execute_plan(plan);

    for part in result {
        let part = part.unwrap();
        // println!("{part:?}");
        let tables = part.get_tables().unwrap();

        for table in tables.iter() {
            println!("{table:#?}");
        }
    }
}