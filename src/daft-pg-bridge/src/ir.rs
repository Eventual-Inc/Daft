//! Rust representation of the frozen `PgDaftPlan` v1 wire contract.

use serde::{Deserialize, Serialize};

/// Top-level physical fragment passed from PostgreSQL/ORCA to the bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgDaftPlanV1 {
    /// SHA-256 digest of the capability registry used while planning.
    pub capability_registry_sha256: String,
    /// Runtime capability bits required by this fragment.
    pub required_runtime_capability_bits: u64,
    /// Non-zero fragment identifier.
    pub fragment_id: u32,
    /// Root node identifier in `nodes`.
    pub root_node_id: u32,
    /// Preferred order in which PostgreSQL-owned sources are driven.
    pub pg_source_drive_order: Vec<u32>,
    /// Descriptors for PostgreSQL-owned Arrow input streams.
    pub sources: Vec<SourceDescriptor>,
    /// Physical operator DAG.
    pub nodes: Vec<Node>,
    /// Contractual schema returned by the fragment root.
    pub output_schema: Vec<Field>,
    /// SHA-256 of canonical payload bytes with this field removed.
    pub payload_sha256: String,
}

/// A PostgreSQL-owned streaming input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDescriptor {
    /// Fragment-local input identifier.
    pub source_id: u32,
    /// Columns supplied by this input.
    pub fields: Vec<Field>,
}

/// A column in a source, node output, or fragment output schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// Stable fragment-local column identifier used by expressions.
    pub column_id: u32,
    /// Stable internal name, never a user/catalog column name.
    pub name: String,
    /// PostgreSQL, Arrow, and Daft type identity.
    #[serde(rename = "type")]
    pub pg_type: PgType,
}

/// Cross-system type identity carried without locale-dependent text conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgType {
    /// PostgreSQL type OID.
    pub pg_oid: u32,
    /// PostgreSQL type modifier (`-1` means no modifier).
    pub typmod: i32,
    /// PostgreSQL collation OID (`0` when not applicable).
    pub collation_oid: u32,
    /// Whether SQL NULL values may occur.
    pub nullable: bool,
    /// Arrow C Data format string.
    pub arrow_format: String,
    /// Frozen Daft data-type name.
    pub daft_dtype: String,
}

/// A named projection/group expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedExpr {
    /// Output column identity.
    pub output: Field,
    /// Computation producing the output column.
    pub expr: Expr,
}

/// Literal wire encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiteralEncoding {
    /// SQL NULL.
    Null,
    /// JSON boolean.
    Bool,
    /// JSON signed integer.
    I64,
    /// Base64-encoded, big-endian binary representation.
    Base64Be,
}

/// Boolean expression operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoolOp {
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
    /// Boolean negation.
    Not,
}

/// Expression variants reserved by protocol v1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Expr {
    /// Reference to a column in the immediate input schema.
    #[serde(rename = "input_ref")]
    InputRef {
        /// Stable input column identifier.
        column_id: u32,
        /// Expected input type; checked against the referenced field.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// Typed literal.
    #[serde(rename = "literal")]
    Literal {
        /// Portable literal encoding.
        encoding: LiteralEncoding,
        /// Encoded literal value.
        value: serde_json::Value,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// AND, OR, or NOT expression.
    #[serde(rename = "bool")]
    Bool {
        /// Boolean operation.
        op: BoolOp,
        /// Operands.
        args: Vec<Expr>,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// Comparison with stable semantic identity and the checked PG OID.
    #[serde(rename = "compare")]
    Compare {
        /// Registry semantic identifier.
        semantic_id: String,
        /// PostgreSQL operator/function OID checked by ORCA.
        pg_oid: u32,
        /// Operands.
        args: Vec<Expr>,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// Arithmetic function/operator call.
    #[serde(rename = "arithmetic")]
    Arithmetic {
        /// Registry semantic identifier.
        semantic_id: String,
        /// PostgreSQL function/operator OID checked by ORCA.
        pg_oid: u32,
        /// Operands.
        args: Vec<Expr>,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// SQL IS NULL or IS NOT NULL.
    #[serde(rename = "null_test")]
    NullTest {
        /// `true` for IS NOT NULL.
        is_not: bool,
        /// Tested expression.
        input: Box<Expr>,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
    /// Semantically identified cast.
    #[serde(rename = "cast")]
    Cast {
        /// Registry semantic identifier.
        semantic_id: String,
        /// Cast input.
        input: Box<Expr>,
        /// Result type.
        #[serde(rename = "type")]
        pg_type: PgType,
    },
}

/// Hash-join key pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HashKey {
    /// Left key expression.
    pub left: Expr,
    /// Right key expression.
    pub right: Expr,
    /// Protocol v1 only permits `false`; the translator enforces it.
    pub null_equals_null: bool,
}

/// Aggregate output expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateExpr {
    /// Registry semantic identifier.
    pub semantic_id: String,
    /// PostgreSQL aggregate OID checked by ORCA.
    pub pg_aggregate_oid: u32,
    /// Aggregate arguments.
    pub args: Vec<Expr>,
    /// Whether SQL DISTINCT is part of this aggregate. Protocol v1 P1 only
    /// admits `false`; carrying the flag prevents the translator from silently
    /// erasing DISTINCT while lowering the expression.
    pub distinct: bool,
    /// Aggregate-local ORDER BY keys. Protocol v1 P1 only admits an empty
    /// vector, but the property is explicit so ordered aggregates fail closed.
    pub order_by: Vec<SortKey>,
    /// SQL FILTER predicate attached to this aggregate. Protocol v1 P1 only
    /// admits `None`.
    pub filter: Option<Expr>,
    /// Aggregate output column.
    pub output: Field,
}

/// PostgreSQL aggregate execution stage represented by a fragment node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateStage {
    /// A complete SQL aggregate, from input rows to final values.
    Complete,
}

/// Grouping shape represented by a protocol-v1 HashAggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupingMode {
    /// One ordinary GROUP BY key list, including the empty list for a global
    /// aggregate. GROUPING SETS, ROLLUP and CUBE are not represented by it.
    Simple,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// PostgreSQL collation semantics admitted for one sort key.
///
/// The actual PostgreSQL collation OID remains in `expr.type.collation_oid`.
/// Keeping only the semantic class here avoids a second, potentially drifting
/// copy of the OID while still making the per-key execution contract explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortCollationSemantics {
    /// The key type is not collatable and its collation OID must be zero.
    None,
    /// Built-in PostgreSQL `C` collation (OID 950 in the frozen PG19 catalog).
    C,
    /// Built-in PostgreSQL `POSIX` collation (OID 951 in the frozen PG19 catalog).
    Posix,
}

/// Sort key and PostgreSQL NULL ordering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortKey {
    /// Key expression.
    pub expr: Expr,
    /// Exact PostgreSQL btree ordering operator selected by ORCA.
    pub pg_sort_operator_oid: u32,
    /// Sort direction.
    pub direction: SortDirection,
    /// Whether NULL sorts before non-NULL values.
    pub nulls_first: bool,
    /// Explicit collation semantics for this key. The value must agree with
    /// the expression type's `collation_oid`.
    pub collation_semantics: SortCollationSemantics,
}

/// Supported hash-join types reserved in protocol v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    /// Inner join.
    Inner,
    /// Left outer join.
    Left,
}

/// Hash table build side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildSide {
    /// Build the left input.
    Left,
    /// Build the right input.
    Right,
}

/// Physical plan node variants reserved by protocol v1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Node {
    /// PostgreSQL-owned streaming source.
    #[serde(rename = "source")]
    Source {
        /// Node identifier.
        id: u32,
        /// Source descriptor identifier.
        source_id: u32,
        /// Source output, repeated for local node validation.
        output: Vec<Field>,
    },
    /// Projection.
    #[serde(rename = "project")]
    Project {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// Ordered output expressions.
        exprs: Vec<NamedExpr>,
    },
    /// Filter (reserved for P1).
    #[serde(rename = "filter")]
    Filter {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// SQL predicate.
        predicate: Expr,
    },
    /// Hash join (reserved for P1).
    #[serde(rename = "hash_join")]
    HashJoin {
        /// Node identifier.
        id: u32,
        /// Left input node.
        left: u32,
        /// Right input node.
        right: u32,
        /// Join type.
        join_type: JoinType,
        /// Build side selected and costed by ORCA.
        build_side: BuildSide,
        /// Equality keys.
        keys: Vec<HashKey>,
        /// Remaining ON predicate.
        join_filter: Option<Expr>,
        /// Predicate applied after join semantics.
        post_filter: Option<Expr>,
    },
    /// Hash aggregate (reserved for P1).
    #[serde(rename = "hash_aggregate")]
    HashAggregate {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// SQL aggregate stage. P1 only accepts a complete aggregate; Daft's
        /// internal partial states are an execution detail and never appear as
        /// PostgreSQL partial/final Aggregate nodes in this field.
        stage: AggregateStage,
        /// SQL grouping form. P1 only accepts ordinary GROUP BY/global
        /// aggregation.
        grouping_mode: GroupingMode,
        /// Grouping expressions.
        group_keys: Vec<NamedExpr>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
    },
    /// Full sort (reserved for P1).
    #[serde(rename = "sort")]
    Sort {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// Ordered sort keys.
        keys: Vec<SortKey>,
    },
    /// Top-N (reserved for P1).
    #[serde(rename = "top_n")]
    TopN {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// Ordered sort keys.
        keys: Vec<SortKey>,
        /// Number of output rows after offset.
        limit: u64,
        /// Optional offset.
        offset: u64,
    },
    /// Constant LIMIT/OFFSET.
    #[serde(rename = "limit")]
    Limit {
        /// Node identifier.
        id: u32,
        /// Input node identifier.
        input: u32,
        /// Rows skipped before returning data.
        offset: u64,
        /// Maximum returned rows; `None` means no count limit.
        count: Option<u64>,
    },
}

impl Node {
    /// Return this node's fragment-local identifier.
    pub fn id(&self) -> u32 {
        match self {
            Self::Source { id, .. }
            | Self::Project { id, .. }
            | Self::Filter { id, .. }
            | Self::HashJoin { id, .. }
            | Self::HashAggregate { id, .. }
            | Self::Sort { id, .. }
            | Self::TopN { id, .. }
            | Self::Limit { id, .. } => *id,
        }
    }

    /// Return all child node identifiers in stable left-to-right order.
    pub fn inputs(&self) -> Vec<u32> {
        match self {
            Self::Source { .. } => Vec::new(),
            Self::HashJoin { left, right, .. } => vec![*left, *right],
            Self::Project { input, .. }
            | Self::Filter { input, .. }
            | Self::HashAggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::TopN { input, .. }
            | Self::Limit { input, .. } => vec![*input],
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{Node, PgDaftPlanV1, SortCollationSemantics, SortDirection};

    fn int4_field() -> Value {
        json!({
            "column_id": 1,
            "name": "_pgd_f1_s0_c1",
            "type": {
                "pg_oid": 23,
                "typmod": -1,
                "collation_oid": 0,
                "nullable": true,
                "arrow_format": "i",
                "daft_dtype": "Int32"
            }
        })
    }

    fn sort_key() -> Value {
        json!({
            "expr": {
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_field()["type"].clone()
            },
            "pg_sort_operator_oid": 521,
            "direction": "desc",
            "nulls_first": true,
            "collation_semantics": "none"
        })
    }

    fn plan_with_root(root: Value) -> Value {
        let field = int4_field();
        json!({
            "capability_registry_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "required_runtime_capability_bits": 15,
            "fragment_id": 1,
            "root_node_id": 2,
            "pg_source_drive_order": [0],
            "sources": [{"source_id": 0, "fields": [field.clone()]}],
            "nodes": [
                {"id": 1, "kind": "source", "source_id": 0, "output": [field.clone()]},
                root
            ],
            "output_schema": [field],
            "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
        })
    }

    #[test]
    fn frozen_source_shape_deserializes() {
        let json = r#"{
          "capability_registry_sha256":"0000000000000000000000000000000000000000000000000000000000000000",
          "required_runtime_capability_bits":15,
          "fragment_id":1,"root_node_id":1,"pg_source_drive_order":[0],
          "sources":[{"source_id":0,"fields":[{"column_id":1,"name":"_pgd_f1_s0_c1","type":{"pg_oid":20,"typmod":-1,"collation_oid":0,"nullable":false,"arrow_format":"l","daft_dtype":"Int64"}}]}],
          "nodes":[{"id":1,"kind":"source","source_id":0,"output":[{"column_id":1,"name":"_pgd_f1_s0_c1","type":{"pg_oid":20,"typmod":-1,"collation_oid":0,"nullable":false,"arrow_format":"l","daft_dtype":"Int64"}}]}],
          "output_schema":[{"column_id":1,"name":"_pgd_f1_s0_c1","type":{"pg_oid":20,"typmod":-1,"collation_oid":0,"nullable":false,"arrow_format":"l","daft_dtype":"Int64"}}],
          "payload_sha256":"0000000000000000000000000000000000000000000000000000000000000000"
        }"#;
        let plan: PgDaftPlanV1 = serde_json::from_str(json).unwrap();
        assert_eq!(plan.nodes[0].id(), 1);
    }

    #[test]
    fn sort_and_topn_require_explicit_per_key_order_semantics() {
        let sort: PgDaftPlanV1 = serde_json::from_value(plan_with_root(json!({
            "id": 2,
            "kind": "sort",
            "input": 1,
            "keys": [sort_key()]
        })))
        .expect("the frozen explicit Sort key shape must deserialize");
        let Node::Sort { keys, .. } = &sort.nodes[1] else {
            panic!("expected Sort root");
        };
        assert_eq!(keys[0].pg_sort_operator_oid, 521);
        assert_eq!(keys[0].direction, SortDirection::Desc);
        assert!(keys[0].nulls_first);
        assert_eq!(keys[0].collation_semantics, SortCollationSemantics::None);

        let top_n: PgDaftPlanV1 = serde_json::from_value(plan_with_root(json!({
            "id": 2,
            "kind": "top_n",
            "input": 1,
            "keys": [sort_key()],
            "limit": 3,
            "offset": 2
        })))
        .expect("TopN must use the same explicit Sort key shape");
        let Node::TopN {
            keys,
            limit,
            offset,
            ..
        } = &top_n.nodes[1]
        else {
            panic!("expected TopN root");
        };
        assert_eq!(keys[0].pg_sort_operator_oid, 521);
        assert_eq!((*limit, *offset), (3, 2));

        for required in [
            "pg_sort_operator_oid",
            "direction",
            "nulls_first",
            "collation_semantics",
        ] {
            let mut implicit = plan_with_root(json!({
                "id": 2,
                "kind": "sort",
                "input": 1,
                "keys": [sort_key()]
            }));
            implicit["nodes"][1]["keys"][0]
                .as_object_mut()
                .expect("sort key object")
                .remove(required);
            let error = serde_json::from_value::<PgDaftPlanV1>(implicit)
                .expect_err("no Sort key property may be implicit");
            assert!(
                error.to_string().contains(required),
                "missing {required} reported as {error}"
            );
        }
    }
}
