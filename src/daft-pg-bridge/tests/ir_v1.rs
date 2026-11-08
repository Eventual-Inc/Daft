//! Contract and plan-shape tests for direct P0 lowering.

use daft_core::prelude::{Literal, Series};
use daft_dsl::AggExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_pg_bridge::{
    capabilities::CAPABILITY_DIGEST,
    ir::PgDaftPlanV1,
    plan::{translate_plan, translate_sort_topn_plan_for_test},
};
use serde_json::{json, Value};

const P0_SOURCE_PROJECT_LIMIT_BITS: u64 = 0b11_1111;
const P1_HASH_JOIN_BITS: u64 = 0b1000_1111;
const P1_HASH_AGGREGATE_BITS: u64 = 0b1_0000_1111;
const P1_SORT_BITS: u64 = 0b10_0000_1111;
const P1_TOP_N_BITS: u64 = 0b100_0000_1111;

fn int8_type() -> Value {
    json!({
        "pg_oid": 20,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": false,
        "arrow_format": "l",
        "daft_dtype": "Int64"
    })
}

fn field(column_id: u32, name: &str) -> Value {
    json!({"column_id": column_id, "name": name, "type": int8_type()})
}

fn nullable_field(column_id: u32, name: &str) -> Value {
    let mut pg_type = int8_type();
    pg_type["nullable"] = json!(true);
    json!({"column_id": column_id, "name": name, "type": pg_type})
}

fn input_ref(column_id: u32, pg_type: Value) -> Value {
    json!({"kind": "input_ref", "column_id": column_id, "type": pg_type})
}

fn pg_type(pg_oid: u32, nullable: bool) -> Value {
    let (arrow_format, daft_dtype) = match pg_oid {
        16 => ("b", "Boolean"),
        20 => ("l", "Int64"),
        21 => ("s", "Int16"),
        23 => ("i", "Int32"),
        1082 => ("tdD", "Date"),
        1114 => ("tsu:", "TimestampMicroseconds"),
        1184 => ("tsu:UTC", "TimestampMicrosecondsUTC"),
        _ => panic!("test fixture does not define PostgreSQL OID {pg_oid}"),
    };
    json!({
        "pg_oid": pg_oid,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": nullable,
        "arrow_format": arrow_format,
        "daft_dtype": daft_dtype
    })
}

fn typed_field(column_id: u32, name: &str, pg_oid: u32, nullable: bool) -> Value {
    json!({"column_id": column_id, "name": name, "type": pg_type(pg_oid, nullable)})
}

fn aggregate_value(grouped: bool) -> Value {
    let int4_group = typed_field(1, "_pgd_f1_s0_c1", 23, true);
    let int2_value = typed_field(2, "_pgd_f1_s0_c2", 21, true);
    let int4_value = typed_field(3, "_pgd_f1_s0_c3", 23, true);
    let group_keys = if grouped {
        json!([{
            "output": typed_field(10, "_pgd_f1_s0_c10", 23, true),
            "expr": input_ref(1, pg_type(23, true))
        }])
    } else {
        json!([])
    };
    let aggregates = json!([
        {
            "semantic_id": "count_star", "pg_aggregate_oid": 2803, "args": [],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(11, "_pgd_f1_s0_c11", 20, false)
        },
        {
            "semantic_id": "count_value", "pg_aggregate_oid": 2147,
            "args": [input_ref(3, pg_type(23, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(12, "_pgd_f1_s0_c12", 20, false)
        },
        {
            "semantic_id": "sum_int2", "pg_aggregate_oid": 2109,
            "args": [input_ref(2, pg_type(21, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(13, "_pgd_f1_s0_c13", 20, true)
        },
        {
            "semantic_id": "sum_int4", "pg_aggregate_oid": 2108,
            "args": [input_ref(3, pg_type(23, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(14, "_pgd_f1_s0_c14", 20, true)
        },
        {
            "semantic_id": "min_int2", "pg_aggregate_oid": 2133,
            "args": [input_ref(2, pg_type(21, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(15, "_pgd_f1_s0_c15", 21, true)
        },
        {
            "semantic_id": "min_int4", "pg_aggregate_oid": 2132,
            "args": [input_ref(3, pg_type(23, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(16, "_pgd_f1_s0_c16", 23, true)
        },
        {
            "semantic_id": "max_int2", "pg_aggregate_oid": 2117,
            "args": [input_ref(2, pg_type(21, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(17, "_pgd_f1_s0_c17", 21, true)
        },
        {
            "semantic_id": "max_int4", "pg_aggregate_oid": 2116,
            "args": [input_ref(3, pg_type(23, true))],
            "distinct": false, "order_by": [], "filter": null,
            "output": typed_field(18, "_pgd_f1_s0_c18", 23, true)
        }
    ]);
    let mut output = Vec::new();
    if grouped {
        output.push(typed_field(10, "_pgd_f1_s0_c10", 23, true));
    }
    for (id, oid, nullable) in [
        (11, 20, false),
        (12, 20, false),
        (13, 20, true),
        (14, 20, true),
        (15, 21, true),
        (16, 23, true),
        (17, 21, true),
        (18, 23, true),
    ] {
        output.push(typed_field(id, &format!("_pgd_f1_s0_c{id}"), oid, nullable));
    }
    json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_HASH_AGGREGATE_BITS,
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [int4_group.clone(), int2_value.clone(), int4_value.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [int4_group, int2_value, int4_value]},
            {
                "id": 2, "kind": "hash_aggregate", "input": 1,
                "stage": "complete", "grouping_mode": "simple",
                "group_keys": group_keys, "aggregates": aggregates
            }
        ],
        "output_schema": output,
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    })
}

fn sort_topn_value(top_n: bool) -> Value {
    let first = typed_field(1, "_pgd_f1_s0_c1", 23, true);
    let second = typed_field(2, "_pgd_f1_s0_c2", 20, false);
    let keys = json!([
        {
            "expr": input_ref(1, pg_type(23, true)),
            "pg_sort_operator_oid": 521,
            "direction": "desc",
            "nulls_first": true,
            "collation_semantics": "none"
        },
        {
            "expr": input_ref(2, pg_type(20, false)),
            "pg_sort_operator_oid": 412,
            "direction": "asc",
            "nulls_first": false,
            "collation_semantics": "none"
        }
    ]);
    let root = if top_n {
        json!({
            "id": 2, "kind": "top_n", "input": 1, "keys": keys,
            "limit": 10, "offset": 2
        })
    } else {
        json!({"id": 2, "kind": "sort", "input": 1, "keys": keys})
    };
    json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": if top_n { P1_TOP_N_BITS } else { P1_SORT_BITS },
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [first.clone(), second.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [first.clone(), second.clone()]},
            root
        ],
        "output_schema": [first, second],
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    })
}

fn single_key_sort_value(pg_oid: u32, pg_sort_operator_oid: u32, direction: &str) -> Value {
    let field = typed_field(1, "_pgd_f1_s0_c1", pg_oid, true);
    json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_SORT_BITS,
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [field.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [field.clone()]},
            {
                "id": 2, "kind": "sort", "input": 1,
                "keys": [{
                    "expr": input_ref(1, pg_type(pg_oid, true)),
                    "pg_sort_operator_oid": pg_sort_operator_oid,
                    "direction": direction,
                    "nulls_first": false,
                    "collation_semantics": "none"
                }]
            }
        ],
        "output_schema": [field],
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    })
}

fn hash_join_value(join_type: &str, build_side: &str, with_post_filter: bool) -> Value {
    let left = field(1, "_pgd_f1_s0_c1");
    let right = field(2, "_pgd_f1_s1_c2");
    let right_output = if join_type == "left" {
        nullable_field(2, "_pgd_f1_s1_c2")
    } else {
        right.clone()
    };
    let post_filter = with_post_filter.then(|| {
        json!({
            "kind": "null_test",
            "is_not": false,
            "input": input_ref(2, right_output["type"].clone()),
            "type": {
                "pg_oid": 16,
                "typmod": -1,
                "collation_oid": 0,
                "nullable": false,
                "arrow_format": "b",
                "daft_dtype": "Boolean"
            }
        })
    });
    json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_HASH_JOIN_BITS,
        "fragment_id": 1,
        "root_node_id": 3,
        "pg_source_drive_order": [0, 1],
        "sources": [
            {"source_id": 0, "fields": [left.clone()]},
            {"source_id": 1, "fields": [right.clone()]}
        ],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [left.clone()]},
            {"id": 2, "kind": "source", "source_id": 1, "output": [right.clone()]},
            {
                "id": 3,
                "kind": "hash_join",
                "left": 1,
                "right": 2,
                "join_type": join_type,
                "build_side": build_side,
                "keys": [{
                    "left": input_ref(1, left["type"].clone()),
                    "right": input_ref(2, right["type"].clone()),
                    "null_equals_null": false
                }],
                "join_filter": null,
                "post_filter": post_filter
            }
        ],
        "output_schema": [left, right_output],
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    })
}

fn project_limit_value() -> Value {
    let source_field = field(1, "_pgd_f1_s0_c1");
    let output_field = field(2, "_pgd_f1_s0_c2");
    json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P0_SOURCE_PROJECT_LIMIT_BITS,
        "fragment_id": 1,
        "root_node_id": 3,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [source_field.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [source_field]},
            {
                "id": 2,
                "kind": "project",
                "input": 1,
                "exprs": [{
                    "output": output_field.clone(),
                    "expr": {"kind": "input_ref", "column_id": 1, "type": int8_type()}
                }]
            },
            {"id": 3, "kind": "limit", "input": 2, "offset": 2, "count": 10}
        ],
        "output_schema": [output_field],
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    })
}

fn parse(value: &Value) -> PgDaftPlanV1 {
    serde_json::from_value(value.clone()).expect("fixture must satisfy the frozen Rust shape")
}

#[test]
fn plan_shape_is_limit_project_streaming_source() {
    let plan = translate_plan(&parse(&project_limit_value())).expect("P0 plan must lower");
    let LocalPhysicalPlan::Limit(limit) = plan.as_ref() else {
        panic!("root must be a Daft Limit, got {plan:?}");
    };
    assert_eq!(limit.limit, 10);
    assert_eq!(limit.offset, Some(2));

    let LocalPhysicalPlan::Project(project) = limit.input.as_ref() else {
        panic!("Limit input must be a Daft Project");
    };
    assert_eq!(project.projection.len(), 1);
    assert_eq!(
        project.schema.field_names().collect::<Vec<_>>(),
        ["_pgd_f1_s0_c2"]
    );

    let LocalPhysicalPlan::StreamingScan(scan) = project.input.as_ref() else {
        panic!("Project input must be a Daft StreamingScan");
    };
    assert_eq!(scan.source_id, 0);
    assert_eq!(
        scan.schema.field_names().collect::<Vec<_>>(),
        ["_pgd_f1_s0_c1"]
    );
}

#[test]
fn plan_shape_binds_by_column_id_not_user_name() {
    let mut value = project_limit_value();
    value["nodes"][1]["exprs"][0]["expr"]["column_id"] = json!(999);
    let error = translate_plan(&parse(&value)).unwrap_err();
    assert!(error.message().contains("PGDAFT_COLUMN_DANGLING"));
}

#[test]
fn plan_shape_rejects_duplicate_nodes_before_lowering() {
    let mut value = project_limit_value();
    value["nodes"][1]["id"] = json!(1);
    let error = translate_plan(&parse(&value)).unwrap_err();
    assert!(error.message().contains("PGDAFT_NODE_DUPLICATE"));
}

#[test]
fn plan_shape_rejects_cycle_and_orphan_nodes() {
    let mut cycle = project_limit_value();
    cycle["nodes"][1]["input"] = json!(3);
    let error = translate_plan(&parse(&cycle)).unwrap_err();
    assert!(error.message().contains("PGDAFT_NODE_CYCLE"));

    let mut orphan = project_limit_value();
    orphan["nodes"].as_array_mut().unwrap().push(json!({
        "id": 4,
        "kind": "limit",
        "input": 1,
        "offset": 0,
        "count": 1
    }));
    let error = translate_plan(&parse(&orphan)).unwrap_err();
    assert!(error.message().contains("PGDAFT_NODE_ORPHAN"));
}

#[test]
fn plan_shape_rejects_wrong_capability_set_and_digest() {
    let mut bits = project_limit_value();
    bits["required_runtime_capability_bits"] = json!(0b1_1111);
    let error = translate_plan(&parse(&bits)).unwrap_err();
    assert!(error.message().contains("PGDAFT_CAPABILITY_SET_MISMATCH"));

    let mut digest = project_limit_value();
    digest["capability_registry_sha256"] =
        json!("0000000000000000000000000000000000000000000000000000000000000000");
    let error = translate_plan(&parse(&digest)).unwrap_err();
    assert!(error
        .message()
        .contains("PGDAFT_CAPABILITY_DIGEST_MISMATCH"));
}

#[test]
fn plan_shape_rejects_invalid_sort_and_unknown_nodes_stably() {
    let mut sort = sort_topn_value(false);
    sort["nodes"][1]["keys"] = json!([]);
    let error = translate_sort_topn_plan_for_test(&parse(&sort)).unwrap_err();
    assert!(error.message().contains("PGDAFT_SORT_KEYS_EMPTY"));

    let mut unknown = project_limit_value();
    unknown["nodes"][1]["kind"] = json!("future_magic");
    let error = serde_json::from_value::<PgDaftPlanV1>(unknown).unwrap_err();
    assert!(error.to_string().contains("unknown variant `future_magic`"));
}

#[test]
fn plan_shape_rejects_source_and_output_schema_mismatches() {
    let mut source = project_limit_value();
    source["nodes"][0]["output"][0]["column_id"] = json!(9);
    let error = translate_plan(&parse(&source)).unwrap_err();
    assert!(error.message().contains("PGDAFT_SOURCE_SCHEMA_MISMATCH"));

    let mut output = project_limit_value();
    output["output_schema"][0]["column_id"] = json!(9);
    let error = translate_plan(&parse(&output)).unwrap_err();
    assert!(error.message().contains("PGDAFT_OUTPUT_SCHEMA_MISMATCH"));
}

#[test]
fn plan_shape_source_only_requires_only_source_prerequisites() {
    let source_field = field(1, "_pgd_f7_s3_c1");
    let value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": 15,
        "fragment_id": 7,
        "root_node_id": 8,
        "pg_source_drive_order": [3],
        "sources": [{"source_id": 3, "fields": [source_field.clone()]}],
        "nodes": [{"id": 8, "kind": "source", "source_id": 3, "output": [source_field.clone()]}],
        "output_schema": [source_field],
        "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    });
    let plan = translate_plan(&parse(&value)).unwrap();
    assert!(matches!(plan.as_ref(), LocalPhysicalPlan::StreamingScan(_)));
}

#[test]
fn hash_join_inner_lowers_explicit_right_build_and_sql_null_equality() {
    let plan = translate_plan(&parse(&hash_join_value("inner", "right", false)))
        .expect("admitted Inner HashJoin must lower directly");
    let LocalPhysicalPlan::HashJoin(join) = plan.as_ref() else {
        panic!("root must be a Daft HashJoin, got {plan:?}");
    };
    assert_eq!(join.build_on_left, Some(false));
    assert_eq!(join.null_equals_null, Some(vec![false]));
    assert_eq!(join.join_type, daft_core::join::JoinType::Inner);
    assert_eq!(join.left_on.len(), 1);
    assert_eq!(join.right_on.len(), 1);
    assert_eq!(
        join.schema.field_names().collect::<Vec<_>>(),
        ["_pgd_f1_s0_c1", "_pgd_f1_s1_c2"]
    );

    let left_build = translate_plan(&parse(&hash_join_value("inner", "left", false)))
        .expect("Inner HashJoin may use the ORCA-selected left build side");
    let LocalPhysicalPlan::HashJoin(left_build) = left_build.as_ref() else {
        panic!("root must remain a Daft HashJoin");
    };
    assert_eq!(left_build.build_on_left, Some(true));
}

#[test]
fn hash_join_left_post_filter_is_above_null_extension() {
    let plan = translate_plan(&parse(&hash_join_value("left", "right", true)))
        .expect("admitted Left HashJoin with costed post_filter must lower");
    let LocalPhysicalPlan::Filter(filter) = plan.as_ref() else {
        panic!("post_filter must be a physical Filter above HashJoin, got {plan:?}");
    };
    let LocalPhysicalPlan::HashJoin(join) = filter.input.as_ref() else {
        panic!("post_filter input must be the completed Left HashJoin");
    };
    assert_eq!(join.join_type, daft_core::join::JoinType::Left);
    assert_eq!(join.build_on_left, Some(false));
    assert_eq!(join.null_equals_null, Some(vec![false]));
    assert_eq!(
        filter.context.phase.as_deref(),
        Some("pg_daft_p1_hash_join_post_filter")
    );
}

#[test]
fn hash_join_rejects_remaining_filter_null_safe_and_unsafe_left_build() {
    let bool_type = json!({
        "pg_oid": 16,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": false,
        "arrow_format": "b",
        "daft_dtype": "Boolean"
    });

    let mut remaining = hash_join_value("inner", "right", false);
    remaining["nodes"][2]["join_filter"] = json!({
        "kind": "literal",
        "encoding": "bool",
        "value": true,
        "type": bool_type
    });
    let error = translate_plan(&parse(&remaining)).unwrap_err();
    assert!(error
        .message()
        .contains("PGDAFT_HASH_JOIN_FILTER_UNSUPPORTED"));

    let mut null_safe = hash_join_value("inner", "right", false);
    null_safe["nodes"][2]["keys"][0]["null_equals_null"] = json!(true);
    let error = translate_plan(&parse(&null_safe)).unwrap_err();
    assert!(error.message().contains("PGDAFT_HASH_JOIN_NULL_EQUALITY"));

    let left_build = hash_join_value("left", "left", false);
    let error = translate_plan(&parse(&left_build)).unwrap_err();
    assert!(error.message().contains("PGDAFT_LEFT_JOIN_BUILD_SIDE"));
}

#[test]
fn hash_join_rejects_empty_float_mismatched_and_unsupported_join() {
    let mut empty = hash_join_value("inner", "right", false);
    empty["nodes"][2]["keys"] = json!([]);
    let error = translate_plan(&parse(&empty)).unwrap_err();
    assert!(error.message().contains("PGDAFT_HASH_JOIN_KEYS_EMPTY"));

    let float_type = json!({
        "pg_oid": 701,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": false,
        "arrow_format": "g",
        "daft_dtype": "Float64"
    });
    let mut float_key = hash_join_value("inner", "right", false);
    float_key["sources"][0]["fields"][0]["type"] = float_type.clone();
    float_key["nodes"][0]["output"][0]["type"] = float_type.clone();
    float_key["nodes"][2]["keys"][0]["left"]["type"] = float_type.clone();
    float_key["output_schema"][0]["type"] = float_type;
    let error = translate_plan(&parse(&float_key)).unwrap_err();
    assert!(error.message().contains("PGDAFT_TYPE_UNSUPPORTED"));

    let mut mismatch = hash_join_value("inner", "right", false);
    mismatch["nodes"][2]["keys"][0]["right"]["type"]["nullable"] = json!(true);
    let error = translate_plan(&parse(&mismatch)).unwrap_err();
    assert!(error.message().contains("PGDAFT_INPUT_TYPE_MISMATCH"));

    let mut unsupported = hash_join_value("inner", "right", false);
    unsupported["nodes"][2]["join_type"] = json!("right");
    let error = serde_json::from_value::<PgDaftPlanV1>(unsupported).unwrap_err();
    assert!(error.to_string().contains("unknown variant `right`"));

    let mut missing_build_side = hash_join_value("inner", "right", false);
    missing_build_side["nodes"][2]
        .as_object_mut()
        .unwrap()
        .remove("build_side");
    let error = serde_json::from_value::<PgDaftPlanV1>(missing_build_side).unwrap_err();
    assert!(error.to_string().contains("missing field `build_side`"));
}

#[test]
fn aggregate_grouped_lowers_all_frozen_functions_without_type_inference() {
    let plan = translate_plan(&parse(&aggregate_value(true)))
        .expect("admitted grouped Aggregate must lower directly");
    let LocalPhysicalPlan::HashAggregate(aggregate) = plan.as_ref() else {
        panic!("root must be a Daft HashAggregate, got {plan:?}");
    };
    assert_eq!(aggregate.group_by.len(), 1);
    assert_eq!(aggregate.aggregations.len(), 8);
    assert_eq!(
        aggregate.schema.field_names().collect::<Vec<_>>(),
        [
            "_pgd_f1_s0_c10",
            "_pgd_f1_s0_c11",
            "_pgd_f1_s0_c12",
            "_pgd_f1_s0_c13",
            "_pgd_f1_s0_c14",
            "_pgd_f1_s0_c15",
            "_pgd_f1_s0_c16",
            "_pgd_f1_s0_c17",
            "_pgd_f1_s0_c18"
        ]
    );
    assert!(matches!(
        aggregate.aggregations[0].as_ref(),
        AggExpr::AggFn { .. }
    ));
    assert!(matches!(
        aggregate.aggregations[1].as_ref(),
        AggExpr::AggFn { .. }
    ));
    assert!(matches!(
        aggregate.aggregations[2].as_ref(),
        AggExpr::AggFn { .. }
    ));
    assert!(matches!(
        aggregate.aggregations[3].as_ref(),
        AggExpr::AggFn { .. }
    ));
    assert!(matches!(
        aggregate.aggregations[4].as_ref(),
        AggExpr::Min(_)
    ));
    assert!(matches!(
        aggregate.aggregations[5].as_ref(),
        AggExpr::Min(_)
    ));
    assert!(matches!(
        aggregate.aggregations[6].as_ref(),
        AggExpr::Max(_)
    ));
    assert!(matches!(
        aggregate.aggregations[7].as_ref(),
        AggExpr::Max(_)
    ));
}

#[test]
fn aggregate_plain_lowers_to_ungrouped_and_checked_sum_combine_overflows() {
    let plan = translate_plan(&parse(&aggregate_value(false)))
        .expect("admitted global Aggregate must lower directly");
    let LocalPhysicalPlan::UnGroupedAggregate(aggregate) = plan.as_ref() else {
        panic!("empty group key list must lower to UnGroupedAggregate, got {plan:?}");
    };
    assert_eq!(aggregate.aggregations.len(), 8);
    assert_eq!(aggregate.schema.len(), 8);

    let AggExpr::AggFn { handle, .. } = aggregate.aggregations[2].as_ref() else {
        panic!("sum_int2 must use the Bridge checked aggregate");
    };
    let totals = Series::from_literals(vec![Literal::Int64(i64::MAX), Literal::Int64(1)])
        .expect("valid int8 states");
    let seen = Series::from_literals(vec![Literal::Boolean(true), Literal::Boolean(true)])
        .expect("valid bool states");
    let error = handle.call_agg_combine(vec![totals, seen]).unwrap_err();
    assert!(error.to_string().contains("PGDAFT_AGG_INT8_OVERFLOW"));
}

#[test]
fn aggregate_rejects_modifier_identity_output_and_group_key_drift() {
    let mut distinct = aggregate_value(true);
    distinct["nodes"][1]["aggregates"][0]["distinct"] = json!(true);
    let error = translate_plan(&parse(&distinct)).unwrap_err();
    assert!(error.message().contains("PGDAFT_AGG_MODIFIER_UNSUPPORTED"));

    let mut identity = aggregate_value(true);
    identity["nodes"][1]["aggregates"][3]["pg_aggregate_oid"] = json!(2109);
    let error = translate_plan(&parse(&identity)).unwrap_err();
    assert!(error.message().contains("PGDAFT_AGG_IDENTITY_UNSUPPORTED"));

    let mut output = aggregate_value(true);
    output["nodes"][1]["aggregates"][0]["output"]["type"]["nullable"] = json!(true);
    output["output_schema"][1]["type"]["nullable"] = json!(true);
    let error = translate_plan(&parse(&output)).unwrap_err();
    assert!(error.message().contains("PGDAFT_AGG_OUTPUT_TYPE"));

    let mut key = aggregate_value(true);
    key["nodes"][1]["group_keys"][0]["expr"]["column_id"] = json!(999);
    let error = translate_plan(&parse(&key)).unwrap_err();
    assert!(error.message().contains("PGDAFT_COLUMN_DANGLING"));
}

#[test]
fn sort_topn_lower_direct_physical_nodes_with_exact_order_vectors() {
    let sort_ir = parse(&sort_topn_value(false));
    let sort = translate_plan(&sort_ir)
        .expect("admitted Sort must construct one direct production physical node");
    let LocalPhysicalPlan::Sort(sort) = sort.as_ref() else {
        panic!("root must be a direct Daft Sort, got {sort:?}");
    };
    assert_eq!(sort.sort_by.len(), 2);
    assert_eq!(sort.descending, [true, false]);
    assert_eq!(sort.nulls_first, [true, false]);
    assert!(matches!(
        sort.input.as_ref(),
        LocalPhysicalPlan::StreamingScan(_)
    ));

    let top_n = translate_plan(&parse(&sort_topn_value(true)))
        .expect("admitted TopN must construct one direct production physical node");
    let LocalPhysicalPlan::TopN(top_n) = top_n.as_ref() else {
        panic!("root must be one direct Daft TopN, not Sort+Limit");
    };
    assert_eq!(top_n.sort_by.len(), 2);
    assert_eq!(top_n.descending, [true, false]);
    assert_eq!(top_n.nulls_first, [true, false]);
    assert_eq!(top_n.limit, 10);
    assert_eq!(top_n.offset, Some(2));
    assert!(matches!(
        top_n.input.as_ref(),
        LocalPhysicalPlan::StreamingScan(_)
    ));
}

#[test]
fn sort_topn_reject_operator_collation_and_limit_drift() {
    let mut operator = sort_topn_value(false);
    operator["nodes"][1]["keys"][0]["pg_sort_operator_oid"] = json!(97);
    let error = translate_sort_topn_plan_for_test(&parse(&operator)).unwrap_err();
    assert!(error.message().contains("PGDAFT_SORT_OPERATOR_MISMATCH"));

    let mut collation = sort_topn_value(false);
    collation["nodes"][1]["keys"][0]["collation_semantics"] = json!("c");
    let error = translate_sort_topn_plan_for_test(&parse(&collation)).unwrap_err();
    assert!(error
        .message()
        .contains("PGDAFT_SORT_COLLATION_UNSUPPORTED"));

    let mut overflow = sort_topn_value(true);
    overflow["nodes"][1]["limit"] = json!(u64::MAX);
    overflow["nodes"][1]["offset"] = json!(1);
    let error = translate_sort_topn_plan_for_test(&parse(&overflow)).unwrap_err();
    assert!(error.message().contains("PGDAFT_TOP_N_LIMIT_OVERFLOW"));
}

#[test]
fn sort_topn_accepts_the_frozen_noncollatable_operator_matrix() {
    for (pg_oid, ascending_oid, descending_oid) in [
        (16, 58, 59),
        (20, 412, 413),
        (21, 95, 520),
        (23, 97, 521),
        (1082, 1095, 1097),
        (1114, 2062, 2064),
        (1184, 1322, 1324),
    ] {
        for (operator_oid, direction, expected_descending) in [
            (ascending_oid, "asc", false),
            (descending_oid, "desc", true),
        ] {
            let plan = translate_sort_topn_plan_for_test(&parse(&single_key_sort_value(
                pg_oid,
                operator_oid,
                direction,
            )))
            .unwrap_or_else(|error| {
                panic!(
                    "OID {pg_oid} operator {operator_oid} should be admitted: {}",
                    error.message()
                )
            });
            let LocalPhysicalPlan::Sort(sort) = plan.as_ref() else {
                panic!("frozen order matrix must lower to Sort, got {plan:?}");
            };
            assert_eq!(sort.descending, [expected_descending]);
            assert_eq!(sort.nulls_first, [false]);
        }
    }
}

#[test]
fn plan_shape_translation_has_no_second_optimizer_entrypoint() {
    let source = include_str!("../src/plan.rs");
    assert!(!source.contains("SQLPlanner"));
    assert!(!source.contains("optimizer::"));
    assert!(!source.contains("daft_local_plan::translate"));
}
