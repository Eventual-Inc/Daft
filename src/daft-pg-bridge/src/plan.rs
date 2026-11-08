//! Validated direct lowering from `PgDaftPlan` to `LocalPhysicalPlan`.
//!
//! This module deliberately does not import or call Daft's logical planner or
//! optimizer. ORCA has already selected the physical operators.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use daft_core::prelude::{Field as DaftField, JoinType as DaftJoinType, Schema};
use daft_dsl::{bound_col, expr::bound_expr::BoundExpr};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;

use crate::{
    capabilities::{RuntimeCapability, CAPABILITY_DIGEST},
    error::{DpgError, DpgErrorCode},
    expr::{
        bind_aggregate_expr, bind_aggregate_group_key, bind_filter_expr, bind_project_expr,
        to_daft_type,
    },
    ir::{
        AggregateStage, BuildSide, Expr, Field, GroupingMode, JoinType as IrJoinType, Node,
        PgDaftPlanV1, SortCollationSemantics, SortDirection, SortKey, SourceDescriptor,
    },
};

const MAX_SOURCES: usize = 64;
const MAX_NODES: usize = 10_000;
const MAX_FIELDS: usize = 4_096;

const BASE_BITS: u64 = RuntimeCapability::PLAN_V1 as u64
    | RuntimeCapability::ARROW_C_DATA_V1 as u64
    | RuntimeCapability::EMBEDDED_SWORDFISH as u64;
const ENABLED_BITS: u64 = BASE_BITS
    | RuntimeCapability::PG_OWNED_SOURCE as u64
    | RuntimeCapability::PROJECT as u64
    | RuntimeCapability::LIMIT as u64
    | RuntimeCapability::FILTER as u64
    | RuntimeCapability::HASH_JOIN as u64
    | RuntimeCapability::HASH_AGGREGATE as u64
    | RuntimeCapability::SORT as u64
    | RuntimeCapability::TOP_N as u64;

struct LoweredNode {
    plan: LocalPhysicalPlanRef,
    fields: Vec<Field>,
}

/// Validate an admitted fragment and directly construct its Daft physical plan.
///
/// Enabled nodes are the P0 `Source`, `Project(input_ref...)`, `Limit` subset,
/// the immutable P1-001-A `Filter` predicate subset, the P1-001-B/C direct
/// Inner/Left equi-HashJoin lowering, and the P1-002 frozen complete
/// HashAggregate subset selected and costed by ORCA, plus the P1-003-C
/// non-collatable Sort/TopN subset whose exact ordering and memory gates have
/// passed the end-to-end admission tests.
/// Other protocol-v1 nodes reserved for later phases deserialize successfully
/// but are rejected with a stable capability diagnostic.
pub fn translate_plan(ir: &PgDaftPlanV1) -> Result<LocalPhysicalPlanRef, Box<DpgError>> {
    translate_plan_with_enabled_bits(ir, ENABLED_BITS)
}

/// Backward-compatible Rust-only entry point retained for the focused
/// P1-003-B lowering tests. P1-003-C advertises the same path in production.
#[doc(hidden)]
pub fn translate_sort_topn_plan_for_test(
    ir: &PgDaftPlanV1,
) -> Result<LocalPhysicalPlanRef, Box<DpgError>> {
    translate_plan(ir)
}

fn translate_plan_with_enabled_bits(
    ir: &PgDaftPlanV1,
    enabled_bits: u64,
) -> Result<LocalPhysicalPlanRef, Box<DpgError>> {
    validate_envelope(ir)?;

    let source_map = build_source_map(ir)?;
    let node_map = build_node_map(ir)?;
    let reachable = validate_graph(ir.root_node_id, &node_map)?;
    if reachable.len() != node_map.len() {
        return Err(invalid(format!(
            "PGDAFT_NODE_ORPHAN: {} node(s) are not reachable from root {}",
            node_map.len() - reachable.len(),
            ir.root_node_id
        )));
    }
    validate_source_cross_references(ir, &node_map, &source_map)?;

    let required_by_nodes = required_bits(&node_map)?;
    if ir.required_runtime_capability_bits != required_by_nodes {
        return Err(invalid(format!(
            "PGDAFT_CAPABILITY_SET_MISMATCH: plan declares {:#x}, nodes require {:#x}",
            ir.required_runtime_capability_bits, required_by_nodes
        )));
    }
    if ir.required_runtime_capability_bits & !enabled_bits != 0 {
        return Err(invalid(format!(
            "PGDAFT_CAPABILITY_UNKNOWN: unsupported runtime bits {:#x}",
            ir.required_runtime_capability_bits & !enabled_bits
        )));
    }

    let lowered = lower_node(ir.root_node_id, ir.fragment_id, &node_map, &source_map)?;
    validate_fields(&ir.output_schema, ir.fragment_id, "fragment output")?;
    if lowered.fields != ir.output_schema {
        return Err(invalid(
            "PGDAFT_OUTPUT_SCHEMA_MISMATCH: fragment output differs from root output".to_string(),
        ));
    }
    if lowered.plan.schema().as_ref() != &schema_from_fields(&ir.output_schema)? {
        return Err(internal(
            "PGDAFT_INTERNAL_SCHEMA_MISMATCH: lowered Daft schema differs from validated IR"
                .to_string(),
        ));
    }
    Ok(lowered.plan)
}

fn validate_envelope(ir: &PgDaftPlanV1) -> Result<(), Box<DpgError>> {
    if ir.fragment_id == 0 {
        return Err(invalid(
            "PGDAFT_FRAGMENT_ID_INVALID: fragment_id must be non-zero".to_string(),
        ));
    }
    if ir.capability_registry_sha256 != CAPABILITY_DIGEST {
        return Err(invalid(format!(
            "PGDAFT_CAPABILITY_DIGEST_MISMATCH: expected {CAPABILITY_DIGEST}"
        )));
    }
    validate_sha256(&ir.payload_sha256, "payload_sha256")?;
    if ir.sources.len() > MAX_SOURCES || ir.pg_source_drive_order.len() > MAX_SOURCES {
        return Err(invalid(format!(
            "PGDAFT_SOURCE_LIMIT: at most {MAX_SOURCES} sources are allowed"
        )));
    }
    if ir.nodes.is_empty() || ir.nodes.len() > MAX_NODES {
        return Err(invalid(format!(
            "PGDAFT_NODE_LIMIT: node count must be in 1..={MAX_NODES}"
        )));
    }
    if ir.output_schema.len() > MAX_FIELDS {
        return Err(invalid(format!(
            "PGDAFT_FIELD_LIMIT: at most {MAX_FIELDS} output fields are allowed"
        )));
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &str) -> Result<(), Box<DpgError>> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(format!(
            "PGDAFT_SHA256_INVALID: {field} must contain 64 lowercase hex digits"
        )));
    }
    Ok(())
}

fn build_source_map<'a>(
    ir: &'a PgDaftPlanV1,
) -> Result<HashMap<u32, &'a SourceDescriptor>, Box<DpgError>> {
    let mut source_map = HashMap::with_capacity(ir.sources.len());
    for source in &ir.sources {
        if source.fields.len() > MAX_FIELDS {
            return Err(invalid(format!(
                "PGDAFT_FIELD_LIMIT: source {} exceeds {MAX_FIELDS} fields",
                source.source_id
            )));
        }
        validate_fields(
            &source.fields,
            ir.fragment_id,
            &format!("source {}", source.source_id),
        )?;
        if source_map.insert(source.source_id, source).is_some() {
            return Err(invalid(format!(
                "PGDAFT_SOURCE_DUPLICATE: source_id {} occurs more than once",
                source.source_id
            )));
        }
    }
    Ok(source_map)
}

fn build_node_map(ir: &PgDaftPlanV1) -> Result<HashMap<u32, &Node>, Box<DpgError>> {
    let mut node_map = HashMap::with_capacity(ir.nodes.len());
    for node in &ir.nodes {
        if node_map.insert(node.id(), node).is_some() {
            return Err(invalid(format!(
                "PGDAFT_NODE_DUPLICATE: node id {} occurs more than once",
                node.id()
            )));
        }
    }
    if !node_map.contains_key(&ir.root_node_id) {
        return Err(invalid(format!(
            "PGDAFT_ROOT_DANGLING: root node {} does not exist",
            ir.root_node_id
        )));
    }
    Ok(node_map)
}

fn validate_graph(
    root: u32,
    node_map: &HashMap<u32, &Node>,
) -> Result<HashSet<u32>, Box<DpgError>> {
    fn visit(
        id: u32,
        node_map: &HashMap<u32, &Node>,
        visiting: &mut HashSet<u32>,
        visited: &mut HashSet<u32>,
    ) -> Result<(), Box<DpgError>> {
        if visited.contains(&id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(invalid(format!(
                "PGDAFT_NODE_CYCLE: cycle reaches node {id}"
            )));
        }
        let node = node_map.get(&id).ok_or_else(|| {
            invalid(format!(
                "PGDAFT_NODE_DANGLING: referenced node {id} does not exist"
            ))
        })?;
        for input in node.inputs() {
            visit(input, node_map, visiting, visited)?;
        }
        visiting.remove(&id);
        visited.insert(id);
        Ok(())
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    visit(root, node_map, &mut visiting, &mut visited)?;
    Ok(visited)
}

fn validate_source_cross_references(
    ir: &PgDaftPlanV1,
    node_map: &HashMap<u32, &Node>,
    source_map: &HashMap<u32, &SourceDescriptor>,
) -> Result<(), Box<DpgError>> {
    let drive: HashSet<u32> = ir.pg_source_drive_order.iter().copied().collect();
    if drive.len() != ir.pg_source_drive_order.len() {
        return Err(invalid(
            "PGDAFT_DRIVE_ORDER_DUPLICATE: source appears more than once".to_string(),
        ));
    }
    let declared: HashSet<u32> = source_map.keys().copied().collect();
    if drive != declared {
        return Err(invalid(
            "PGDAFT_DRIVE_ORDER_MISMATCH: drive order must list every source exactly once"
                .to_string(),
        ));
    }

    let mut source_nodes = HashSet::new();
    for node in node_map.values() {
        if let Node::Source {
            source_id, output, ..
        } = node
        {
            let source = source_map.get(source_id).ok_or_else(|| {
                invalid(format!(
                    "PGDAFT_SOURCE_DANGLING: source node references source_id {source_id}"
                ))
            })?;
            if !source_nodes.insert(*source_id) {
                return Err(invalid(format!(
                    "PGDAFT_SOURCE_NODE_DUPLICATE: source_id {source_id} has multiple source nodes"
                )));
            }
            if output != &source.fields {
                return Err(invalid(format!(
                    "PGDAFT_SOURCE_SCHEMA_MISMATCH: source node output differs for source_id {source_id}"
                )));
            }
        }
    }
    if source_nodes != declared {
        return Err(invalid(
            "PGDAFT_SOURCE_NODE_MISMATCH: every source needs exactly one source node".to_string(),
        ));
    }
    Ok(())
}

fn required_bits(node_map: &HashMap<u32, &Node>) -> Result<u64, Box<DpgError>> {
    let mut bits = BASE_BITS;
    for node in node_map.values() {
        match node {
            Node::Source { .. } => bits |= RuntimeCapability::PG_OWNED_SOURCE as u64,
            Node::Project { .. } => bits |= RuntimeCapability::PROJECT as u64,
            Node::Limit { .. } => bits |= RuntimeCapability::LIMIT as u64,
            Node::Filter { .. } => bits |= RuntimeCapability::FILTER as u64,
            Node::HashJoin { .. } => bits |= RuntimeCapability::HASH_JOIN as u64,
            Node::HashAggregate { .. } => bits |= RuntimeCapability::HASH_AGGREGATE as u64,
            Node::Sort { .. } => bits |= RuntimeCapability::SORT as u64,
            Node::TopN { .. } => bits |= RuntimeCapability::TOP_N as u64,
        }
    }
    Ok(bits)
}

fn lower_node(
    id: u32,
    fragment_id: u32,
    node_map: &HashMap<u32, &Node>,
    source_map: &HashMap<u32, &SourceDescriptor>,
) -> Result<LoweredNode, Box<DpgError>> {
    let node = node_map
        .get(&id)
        .expect("graph validation guarantees that all lowered nodes exist");
    match node {
        Node::Source {
            source_id, output, ..
        } => {
            let source = source_map
                .get(source_id)
                .expect("source cross-validation guarantees descriptor existence");
            let schema = Arc::new(schema_from_fields(&source.fields)?);
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::streaming_scan(
                    *source_id,
                    schema,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p0_source"),
                ),
                fields: output.clone(),
            })
        }
        Node::Project { input, exprs, .. } => {
            if exprs.len() > MAX_FIELDS {
                return Err(invalid(format!(
                    "PGDAFT_FIELD_LIMIT: Project {id} exceeds {MAX_FIELDS} expressions"
                )));
            }
            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            let fields: Vec<Field> = exprs.iter().map(|named| named.output.clone()).collect();
            validate_fields(&fields, fragment_id, "Project output")?;
            let projection = exprs
                .iter()
                .map(|named| bind_project_expr(&named.expr, &input.fields, &named.output))
                .collect::<Result<Vec<_>, _>>()?;
            let schema = Arc::new(schema_from_fields(&fields)?);
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::project(
                    input.plan,
                    projection,
                    schema,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p0_project"),
                ),
                fields,
            })
        }
        Node::Limit {
            input,
            offset,
            count,
            ..
        } => {
            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::limit(
                    input.plan,
                    count.unwrap_or(u64::MAX),
                    Some(*offset),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p0_limit"),
                ),
                fields: input.fields,
            })
        }
        Node::Filter {
            input, predicate, ..
        } => {
            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            let predicate = bind_filter_expr(predicate, &input.fields)?;
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::filter(
                    input.plan,
                    predicate,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p1_filter"),
                ),
                fields: input.fields,
            })
        }
        Node::HashJoin {
            left,
            right,
            join_type,
            build_side,
            keys,
            join_filter,
            post_filter,
            ..
        } => {
            if left == right {
                return Err(invalid(
                    "PGDAFT_HASH_JOIN_SHARED_INPUT: left and right must be distinct nodes"
                        .to_string(),
                ));
            }
            if keys.is_empty() {
                return Err(invalid(
                    "PGDAFT_HASH_JOIN_KEYS_EMPTY: equi HashJoin requires at least one key"
                        .to_string(),
                ));
            }
            if join_filter.is_some() {
                return Err(invalid(
                    "PGDAFT_HASH_JOIN_FILTER_UNSUPPORTED: P1 remaining join_filter must be null"
                        .to_string(),
                ));
            }
            if *join_type == IrJoinType::Left && *build_side != BuildSide::Right {
                return Err(invalid(
                    "PGDAFT_LEFT_JOIN_BUILD_SIDE: P1 Left Join must build the non-preserved right input"
                        .to_string(),
                ));
            }

            let left = lower_node(*left, fragment_id, node_map, source_map)?;
            let right = lower_node(*right, fragment_id, node_map, source_map)?;
            let mut left_on = Vec::with_capacity(keys.len());
            let mut right_on = Vec::with_capacity(keys.len());
            for key in keys {
                if key.null_equals_null {
                    return Err(invalid(
                        "PGDAFT_HASH_JOIN_NULL_EQUALITY: ordinary equality requires null_equals_null=false"
                            .to_string(),
                    ));
                }
                let (left_key, left_type) = bind_hash_join_key(&key.left, &left.fields, "left")?;
                let (right_key, right_type) =
                    bind_hash_join_key(&key.right, &right.fields, "right")?;
                if !same_hash_join_type(&left_type, &right_type) {
                    return Err(invalid(
                        "PGDAFT_HASH_JOIN_KEY_TYPE_MISMATCH: left and right key types differ"
                            .to_string(),
                    ));
                }
                left_on.push(left_key);
                right_on.push(right_key);
            }

            let fields = hash_join_output_fields(&left.fields, &right.fields, *join_type);
            validate_fields(&fields, fragment_id, "HashJoin output")?;
            let schema = Arc::new(schema_from_fields(&fields)?);
            let join = LocalPhysicalPlan::hash_join(
                left.plan,
                right.plan,
                left_on,
                right_on,
                Some(*build_side == BuildSide::Left),
                Some(vec![false; keys.len()]),
                match join_type {
                    IrJoinType::Inner => DaftJoinType::Inner,
                    IrJoinType::Left => DaftJoinType::Left,
                },
                schema,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p1_hash_join"),
            );

            if let Some(predicate) = post_filter {
                let predicate = bind_filter_expr(predicate, &fields)?;
                Ok(LoweredNode {
                    plan: LocalPhysicalPlan::filter(
                        join,
                        predicate,
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(id as usize))
                            .with_phase("pg_daft_p1_hash_join_post_filter"),
                    ),
                    fields,
                })
            } else {
                Ok(LoweredNode { plan: join, fields })
            }
        }
        Node::HashAggregate {
            input,
            stage,
            grouping_mode,
            group_keys,
            aggregates,
            ..
        } => {
            if *stage != AggregateStage::Complete || *grouping_mode != GroupingMode::Simple {
                return Err(invalid(
                    "PGDAFT_AGG_STAGE_UNSUPPORTED: P1 requires complete/simple aggregation"
                        .to_string(),
                ));
            }
            if aggregates.is_empty() {
                return Err(invalid(
                    "PGDAFT_AGG_EMPTY: HashAggregate requires at least one aggregate expression"
                        .to_string(),
                ));
            }
            if group_keys.len() + aggregates.len() > MAX_FIELDS {
                return Err(invalid(format!(
                    "PGDAFT_FIELD_LIMIT: HashAggregate {id} exceeds {MAX_FIELDS} output fields"
                )));
            }

            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            let group_by = group_keys
                .iter()
                .map(|key| bind_aggregate_group_key(key, &input.fields))
                .collect::<Result<Vec<_>, _>>()?;
            let aggregations = aggregates
                .iter()
                .map(|aggregate| {
                    bind_aggregate_expr(aggregate, &input.fields, input.plan.schema().as_ref())
                })
                .collect::<Result<Vec<_>, _>>()?;

            let mut fields = Vec::with_capacity(group_keys.len() + aggregates.len());
            fields.extend(group_keys.iter().map(|key| key.output.clone()));
            fields.extend(aggregates.iter().map(|aggregate| aggregate.output.clone()));
            validate_fields(&fields, fragment_id, "HashAggregate output")?;
            let schema = Arc::new(schema_from_fields(&fields)?);
            let context =
                LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p1_hash_aggregate");
            let plan = if group_by.is_empty() {
                LocalPhysicalPlan::ungrouped_aggregate(
                    input.plan,
                    aggregations,
                    schema,
                    StatsState::NotMaterialized,
                    context,
                )
            } else {
                LocalPhysicalPlan::hash_aggregate(
                    input.plan,
                    aggregations,
                    group_by,
                    schema,
                    StatsState::NotMaterialized,
                    context,
                )
            };
            Ok(LoweredNode { plan, fields })
        }
        Node::Sort { input, keys, .. } => {
            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            let (sort_by, descending, nulls_first) = bind_sort_keys(keys, &input.fields)?;
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::sort(
                    input.plan,
                    sort_by,
                    descending,
                    nulls_first,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p1_sort"),
                ),
                fields: input.fields,
            })
        }
        Node::TopN {
            input,
            keys,
            limit,
            offset,
            ..
        } => {
            limit.checked_add(*offset).ok_or_else(|| {
                invalid("PGDAFT_TOP_N_LIMIT_OVERFLOW: limit + offset exceeds u64".to_string())
            })?;
            usize::try_from(*limit).map_err(|_| {
                invalid("PGDAFT_TOP_N_LIMIT_RANGE: limit exceeds usize".to_string())
            })?;
            usize::try_from(*offset).map_err(|_| {
                invalid("PGDAFT_TOP_N_OFFSET_RANGE: offset exceeds usize".to_string())
            })?;

            let input = lower_node(*input, fragment_id, node_map, source_map)?;
            let (sort_by, descending, nulls_first) = bind_sort_keys(keys, &input.fields)?;
            Ok(LoweredNode {
                plan: LocalPhysicalPlan::top_n(
                    input.plan,
                    sort_by,
                    descending,
                    nulls_first,
                    *limit,
                    Some(*offset),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(id as usize)).with_phase("pg_daft_p1_top_n"),
                ),
                fields: input.fields,
            })
        }
    }
}

fn bind_sort_keys(
    keys: &[SortKey],
    input_fields: &[Field],
) -> Result<(Vec<BoundExpr>, Vec<bool>, Vec<bool>), Box<DpgError>> {
    if keys.is_empty() {
        return Err(invalid(
            "PGDAFT_SORT_KEYS_EMPTY: Sort/TopN requires at least one key".to_string(),
        ));
    }
    let mut sort_by = Vec::with_capacity(keys.len());
    let mut descending = Vec::with_capacity(keys.len());
    let mut nulls_first = Vec::with_capacity(keys.len());
    for key in keys {
        let Expr::InputRef { column_id, pg_type } = &key.expr else {
            return Err(invalid(
                "PGDAFT_SORT_KEY_EXPR: P1 key must be an input_ref".to_string(),
            ));
        };
        let expected_operator = match (pg_type.pg_oid, key.direction) {
            (16, SortDirection::Asc) => 58,
            (16, SortDirection::Desc) => 59,
            (20, SortDirection::Asc) => 412,
            (20, SortDirection::Desc) => 413,
            (21, SortDirection::Asc) => 95,
            (21, SortDirection::Desc) => 520,
            (23, SortDirection::Asc) => 97,
            (23, SortDirection::Desc) => 521,
            (1082, SortDirection::Asc) => 1095,
            (1082, SortDirection::Desc) => 1097,
            (1114, SortDirection::Asc) => 2062,
            (1114, SortDirection::Desc) => 2064,
            (1184, SortDirection::Asc) => 1322,
            (1184, SortDirection::Desc) => 1324,
            _ => {
                return Err(invalid(format!(
                    "PGDAFT_SORT_KEY_TYPE_UNSUPPORTED: PostgreSQL type OID {} is not admitted",
                    pg_type.pg_oid
                )));
            }
        };
        if key.pg_sort_operator_oid != expected_operator {
            return Err(invalid(format!(
                "PGDAFT_SORT_OPERATOR_MISMATCH: expected operator {expected_operator}, got {}",
                key.pg_sort_operator_oid
            )));
        }
        if key.collation_semantics != SortCollationSemantics::None || pg_type.collation_oid != 0 {
            return Err(invalid(
                "PGDAFT_SORT_COLLATION_UNSUPPORTED: P1-003-B admits only non-collatable keys"
                    .to_string(),
            ));
        }

        let mut matches = input_fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.column_id == *column_id);
        let Some((index, input)) = matches.next() else {
            return Err(invalid(format!(
                "PGDAFT_COLUMN_DANGLING: sort key column_id {column_id} does not exist"
            )));
        };
        if matches.next().is_some() || &input.pg_type != pg_type {
            return Err(invalid(format!(
                "PGDAFT_INPUT_TYPE_MISMATCH: sort key column_id {column_id} is ambiguous or has a different type"
            )));
        }
        let field = DaftField::new(input.name.as_str(), to_daft_type(pg_type)?);
        sort_by.push(BoundExpr::new_unchecked(bound_col(index, field)));
        descending.push(key.direction == SortDirection::Desc);
        nulls_first.push(key.nulls_first);
    }
    Ok((sort_by, descending, nulls_first))
}

fn bind_hash_join_key(
    expr: &Expr,
    input_fields: &[Field],
    side: &str,
) -> Result<(BoundExpr, crate::ir::PgType), Box<DpgError>> {
    let Expr::InputRef { column_id, pg_type } = expr else {
        return Err(invalid(format!(
            "PGDAFT_HASH_JOIN_KEY_EXPR: {side} equi key must be an input_ref"
        )));
    };
    if !matches!(pg_type.pg_oid, 16 | 20 | 21 | 23) {
        return Err(invalid(format!(
            "PGDAFT_HASH_JOIN_KEY_TYPE_UNSUPPORTED: PostgreSQL type OID {} is not admitted",
            pg_type.pg_oid
        )));
    }
    let mut matches = input_fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.column_id == *column_id);
    let Some((index, input)) = matches.next() else {
        return Err(invalid(format!(
            "PGDAFT_COLUMN_DANGLING: {side} key column_id {column_id} does not exist"
        )));
    };
    if matches.next().is_some() || &input.pg_type != pg_type {
        return Err(invalid(format!(
            "PGDAFT_INPUT_TYPE_MISMATCH: {side} key column_id {column_id} is ambiguous or has a different type"
        )));
    }
    let field = DaftField::new(input.name.as_str(), to_daft_type(pg_type)?);
    Ok((
        BoundExpr::new_unchecked(bound_col(index, field)),
        pg_type.clone(),
    ))
}

fn same_hash_join_type(left: &crate::ir::PgType, right: &crate::ir::PgType) -> bool {
    left.pg_oid == right.pg_oid
        && left.typmod == right.typmod
        && left.collation_oid == right.collation_oid
        && left.arrow_format == right.arrow_format
        && left.daft_dtype == right.daft_dtype
}

fn hash_join_output_fields(left: &[Field], right: &[Field], join_type: IrJoinType) -> Vec<Field> {
    let mut fields = Vec::with_capacity(left.len() + right.len());
    fields.extend_from_slice(left);
    fields.extend(right.iter().cloned().map(|mut field| {
        if join_type == IrJoinType::Left {
            field.pg_type.nullable = true;
        }
        field
    }));
    fields
}

fn validate_fields(fields: &[Field], fragment_id: u32, owner: &str) -> Result<(), Box<DpgError>> {
    if fields.len() > MAX_FIELDS {
        return Err(invalid(format!(
            "PGDAFT_FIELD_LIMIT: {owner} exceeds {MAX_FIELDS} fields"
        )));
    }
    let mut ids = HashSet::with_capacity(fields.len());
    let mut names = HashSet::with_capacity(fields.len());
    for field in fields {
        if !ids.insert(field.column_id) {
            return Err(invalid(format!(
                "PGDAFT_COLUMN_DUPLICATE: {owner} repeats column_id {}",
                field.column_id
            )));
        }
        if !names.insert(field.name.as_str()) {
            return Err(invalid(format!(
                "PGDAFT_COLUMN_NAME_DUPLICATE: {owner} repeats name {}",
                field.name
            )));
        }
        if stable_name_fragment(&field.name) != Some(fragment_id) {
            return Err(invalid(format!(
                "PGDAFT_COLUMN_NAME_INVALID: '{}' is not a stable name for fragment {fragment_id}",
                field.name
            )));
        }
        to_daft_type(&field.pg_type)?;
    }
    Ok(())
}

fn stable_name_fragment(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("_pgd_f")?;
    let (fragment, rest) = rest.split_once("_s")?;
    let (source, column) = rest.split_once("_c")?;
    if fragment.is_empty()
        || source.is_empty()
        || column.is_empty()
        || !fragment.bytes().all(|b| b.is_ascii_digit())
        || !source.bytes().all(|b| b.is_ascii_digit())
        || !column.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    fragment.parse().ok()
}

fn schema_from_fields(fields: &[Field]) -> Result<Schema, Box<DpgError>> {
    fields
        .iter()
        .map(|field| {
            Ok(DaftField::new(
                field.name.as_str(),
                to_daft_type(&field.pg_type)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<DpgError>>>()
        .map(Schema::new)
}

fn invalid(message: String) -> Box<DpgError> {
    DpgError::new(DpgErrorCode::InvalidArg, message)
}

fn internal(message: String) -> Box<DpgError> {
    DpgError::new(DpgErrorCode::InternalError, message)
}
