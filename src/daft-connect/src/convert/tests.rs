use std::{collections::HashMap, io::Write};

use spark_connect::{
    expression::{
        literal::LiteralType, Alias, ExprType, Literal, UnresolvedAttribute, UnresolvedFunction,
    },
    read::{DataSource, ReadType},
    relation::RelType,
    Expression, Filter, Read, Relation, RelationCommon, ShowString, WithColumns,
};

use crate::{command::execute_plan, convert::to_logical_plan};

#[test]
pub fn test_filter() {
    let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
    let bytes = include_bytes!("../../tests/increasing_id_data.parquet");
    tmp_file.write_all(bytes).unwrap();

    let path = tmp_file.path().to_str().unwrap();

    #[expect(
        deprecated,
        reason = "Some of the fields are deprecated, but we must still set them to their default values."
    )]
    #[rustfmt::skip] // rustfmt gets stuck on this (perhaps) forever. Stuck on this for literal minutes 😅
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
                                                                                                path.to_string(),
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

    let plan = to_logical_plan(input).unwrap().build();

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
