use std::collections::HashMap;

use daft_core::{lit::Literal, prelude::*, series::IntoSeries};
use daft_dsl::{ExprRef, lit, unresolved_col};
use onnx_rs::ast;

use crate::{
    error::{OnnxPlannerError, OnnxPlannerResult},
    ops::onnx_op_to_expr,
};

pub struct OnnxPlanner;

impl OnnxPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Translate an ONNX model into a list of aliased Daft expressions (one per graph output).
    ///
    /// `inputs` are bound positionally to the graph's non-initializer inputs.
    pub fn plan(&self, model: &ast::Model, inputs: &[ExprRef]) -> OnnxPlannerResult<Vec<ExprRef>> {
        let graph = model
            .graph
            .as_ref()
            .ok_or_else(|| OnnxPlannerError::InvalidGraph {
                message: "model has no graph".to_string(),
            })?;
        self.plan_graph(graph, inputs)
    }

    fn plan_graph(&self, graph: &ast::Graph, user_inputs: &[ExprRef]) -> OnnxPlannerResult<Vec<ExprRef>> {
        let mut values: HashMap<&str, ExprRef> = HashMap::new();

        let graph_inputs = graph.non_init_inputs();
        if user_inputs.len() != graph_inputs.len() {
            return Err(OnnxPlannerError::InvalidGraph {
                message: format!(
                    "model expects {} input(s) [{}], but got {}",
                    graph_inputs.len(),
                    graph_inputs.iter().map(|i| i.name).collect::<Vec<_>>().join(", "),
                    user_inputs.len(),
                ),
            });
        }
        for (graph_input, user_expr) in graph_inputs.iter().zip(user_inputs.iter()) {
            values.insert(graph_input.name, user_expr.clone());
        }

        for init in &graph.initializer {
            let lit_expr = tensor_proto_to_literal(init)?;
            values.insert(init.name(), lit_expr);
        }

        for node in &graph.node {
            let input_exprs: Vec<ExprRef> = node
                .input
                .iter()
                .filter(|name| !name.is_empty())
                .map(|name| {
                    values
                        .get(name)
                        .cloned()
                        .ok_or_else(|| OnnxPlannerError::MissingInput {
                            node: node.name.to_string(),
                            input: name.to_string(),
                        })
                })
                .collect::<OnnxPlannerResult<Vec<_>>>()?;

            let expr = onnx_op_to_expr(&node.op_type, input_exprs)?;

            for output_name in &node.output {
                if !output_name.is_empty() {
                    values.insert(output_name, expr.clone());
                }
            }
        }

        let output_exprs: Vec<ExprRef> = graph
            .output
            .iter()
            .map(|output| {
                let expr = values.get(output.name).cloned().ok_or_else(|| {
                    OnnxPlannerError::InvalidGraph {
                        message: format!(
                            "graph output '{}' not found in computed values",
                            output.name
                        ),
                    }
                })?;
                Ok(expr.alias(output.name))
            })
            .collect::<OnnxPlannerResult<Vec<_>>>()?;

        Ok(output_exprs)
    }
}

/// Convert an ONNX TensorProto (initializer) into a Daft tensor literal expression.
fn tensor_proto_to_literal(tensor: &ast::TensorProto) -> OnnxPlannerResult<ExprRef> {
    let shape: Vec<u64> = tensor.dims().iter().map(|d| *d as u64).collect();
    let field = Field::new(tensor.name(), onnx_data_type_to_daft(&tensor.data_type())?);

    let data = match tensor.data_type() {
        ast::DataType::Float => {
            let values = tensor
                .as_f32()
                .ok_or_else(|| OnnxPlannerError::InvalidGraph {
                    message: format!("no float data for initializer '{}'", tensor.name()),
                })?;
            Float32Array::from_iter(field, values.iter().copied().map(Some)).into_series()
        }
        ast::DataType::Double => {
            let values = tensor
                .as_f64()
                .ok_or_else(|| OnnxPlannerError::InvalidGraph {
                    message: format!("no double data for initializer '{}'", tensor.name()),
                })?;
            Float64Array::from_iter(field, values.iter().copied().map(Some)).into_series()
        }
        ast::DataType::Int32 => {
            let values = tensor
                .as_i32()
                .ok_or_else(|| OnnxPlannerError::InvalidGraph {
                    message: format!("no int32 data for initializer '{}'", tensor.name()),
                })?;
            Int32Array::from_iter(field, values.iter().copied().map(Some)).into_series()
        }
        ast::DataType::Int64 => {
            let values = tensor
                .as_i64()
                .ok_or_else(|| OnnxPlannerError::InvalidGraph {
                    message: format!("no int64 data for initializer '{}'", tensor.name()),
                })?;
            Int64Array::from_iter(field, values.iter().copied().map(Some)).into_series()
        }
        dt => {
            return Err(OnnxPlannerError::InvalidGraph {
                message: format!(
                    "unsupported initializer data type {:?} for '{}'",
                    dt,
                    tensor.name()
                ),
            });
        }
    };

    Ok(lit(Literal::Tensor { data, shape }).into())
}

fn onnx_data_type_to_daft(dt: &ast::DataType) -> OnnxPlannerResult<DataType> {
    match dt {
        ast::DataType::Float => Ok(DataType::Float32),
        ast::DataType::Double => Ok(DataType::Float64),
        ast::DataType::Int32 => Ok(DataType::Int32),
        ast::DataType::Int64 => Ok(DataType::Int64),
        ast::DataType::Int16 => Ok(DataType::Int16),
        ast::DataType::Int8 => Ok(DataType::Int8),
        ast::DataType::Uint8 => Ok(DataType::UInt8),
        ast::DataType::Uint16 => Ok(DataType::UInt16),
        ast::DataType::Uint32 => Ok(DataType::UInt32),
        ast::DataType::Uint64 => Ok(DataType::UInt64),
        ast::DataType::Bool => Ok(DataType::Boolean),
        ast::DataType::Float16 | ast::DataType::Bfloat16 => Ok(DataType::Float32),
        other => Err(OnnxPlannerError::InvalidGraph {
            message: format!("unsupported tensor element type: {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use super::*;
    use crate::{
        onnx::{load_onnx_bytes, parse_model},
        ops::register_onnx_ops,
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(register_onnx_ops);
    }

    #[test]
    fn test_dbg() {
        setup();
        let bytes = load_onnx_bytes("../../tests/assets/onnx/mlp.onnx", None).unwrap();
        let model = parse_model(&bytes).unwrap();

        let planner = OnnxPlanner::new();
        let inputs: Vec<ExprRef> = model
            .graph
            .as_ref()
            .unwrap()
            .non_init_inputs()
            .iter()
            .map(|i| unresolved_col(i.name))
            .collect();
        let expr = planner.plan(&model, &inputs).unwrap();
        dbg!(&expr);
        let expr_str = format!("{expr:?}");
        eprintln!("expr_str: {expr_str}");
        assert!(false);
    }

    #[test]
    fn test_plan_real_mlp() {
        setup();
        let bytes = load_onnx_bytes("../../tests/assets/onnx/mlp.onnx", None).unwrap();
        let model = parse_model(&bytes).unwrap();

        let planner = OnnxPlanner::new();
        let inputs: Vec<ExprRef> = model
            .graph
            .as_ref()
            .unwrap()
            .non_init_inputs()
            .iter()
            .map(|i| unresolved_col(i.name))
            .collect();
        let exprs = planner.plan(&model, &inputs).unwrap();
        assert_eq!(exprs.len(), 1);

        let expr_str = format!("{:?}", exprs[0]);
        assert!(expr_str.contains("onnx.MatMul"));
        assert!(expr_str.contains("onnx.Add"));
        assert!(expr_str.contains("onnx.Relu"));
    }

    #[test]
    fn test_plan_real_residual_block() {
        setup();
        let bytes = load_onnx_bytes("../../tests/assets/onnx/residual_block.onnx", None).unwrap();
        let model = parse_model(&bytes).unwrap();

        let planner = OnnxPlanner::new();
        let inputs: Vec<ExprRef> = model
            .graph
            .as_ref()
            .unwrap()
            .non_init_inputs()
            .iter()
            .map(|i| unresolved_col(i.name))
            .collect();
        let exprs = planner.plan(&model, &inputs).unwrap();
        assert_eq!(exprs.len(), 1);

        let expr_str = format!("{:?}", exprs[0]);
        assert!(expr_str.contains("onnx.Conv"));
        assert!(expr_str.contains("onnx.BatchNormalization"));
        assert!(expr_str.contains("onnx.Relu"));
        assert!(expr_str.contains("onnx.Add"));
    }

    #[test]
    fn test_unsupported_op() {
        setup();
        let graph = ast::Graph {
            input: vec![ast::ValueInfo {
                name: "X",
                r#type: Some(ast::TypeProto {
                    value: Some(ast::TypeValue::Tensor(ast::TensorTypeProto {
                        elem_type: ast::DataType::Float,
                        shape: None,
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            output: vec![ast::ValueInfo {
                name: "Y",
                ..Default::default()
            }],
            node: vec![ast::Node {
                op_type: ast::OpType::Custom("MadeUpOp"),
                input: vec!["X"],
                output: vec!["Y"],
                ..Default::default()
            }],
            ..Default::default()
        };

        let model = ast::Model {
            graph: Some(graph),
            ..Default::default()
        };

        let planner = OnnxPlanner::new();
        let result = planner.plan(&model, &[unresolved_col("X")]);
        assert!(matches!(
            result,
            Err(OnnxPlannerError::UnsupportedOp { .. })
        ));
    }
}
