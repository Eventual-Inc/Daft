use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::*,
    series::Series,
};
use daft_dsl::{
    Expr, ExprRef,
    functions::{
        BuiltinScalarFn, FunctionArg, FunctionArgs, FUNCTION_REGISTRY,
        ScalarUDF,
        scalar::{EvalContext, ScalarFn},
    },
};
use serde::{Deserialize, Serialize};

use crate::error::{OnnxPlannerError, OnnxPlannerResult};

macro_rules! define_onnx_op {
    ($struct_name:ident, $op_name:literal) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $struct_name;

        #[typetag::serde]
        impl ScalarUDF for $struct_name {
            fn name(&self) -> &'static str {
                concat!("onnx.", $op_name)
            }

            fn call(
                &self,
                _inputs: FunctionArgs<Series>,
                _ctx: &EvalContext,
            ) -> DaftResult<Series> {
                Err(DaftError::ComputeError(format!(
                    "ONNX op '{}' is a planning stub — execution not yet implemented",
                    $op_name
                )))
            }

            fn get_return_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                let first = inputs.required((0, "input"))?;
                first.to_field(schema)
            }
        }
    };
}

macro_rules! register_onnx_ops {
    ($($struct_name:ident => $op_name:literal),+ $(,)?) => {
        $(define_onnx_op!($struct_name, $op_name);)+

        pub fn register_onnx_ops() {
            let mut registry = FUNCTION_REGISTRY.write().unwrap();
            $(registry.add_fn($struct_name);)+
        }
    };
}

register_onnx_ops! {
    // Activation
    OnnxRelu => "Relu",
    OnnxLeakyRelu => "LeakyRelu",
    OnnxSigmoid => "Sigmoid",
    OnnxTanh => "Tanh",
    OnnxSoftmax => "Softmax",
    OnnxLogSoftmax => "LogSoftmax",
    OnnxElu => "Elu",
    OnnxSelu => "Selu",
    OnnxPRelu => "PRelu",
    OnnxGelu => "Gelu",
    OnnxHardSigmoid => "HardSigmoid",
    OnnxHardSwish => "HardSwish",
    OnnxSoftplus => "Softplus",
    OnnxSoftsign => "Softsign",
    OnnxThresholdedRelu => "ThresholdedRelu",
    OnnxCelu => "Celu",
    OnnxMish => "Mish",

    // Math (element-wise)
    OnnxAdd => "Add",
    OnnxSub => "Sub",
    OnnxMul => "Mul",
    OnnxDiv => "Div",
    OnnxNeg => "Neg",
    OnnxAbs => "Abs",
    OnnxSqrt => "Sqrt",
    OnnxExp => "Exp",
    OnnxLog => "Log",
    OnnxPow => "Pow",
    OnnxCeil => "Ceil",
    OnnxFloor => "Floor",
    OnnxRound => "Round",
    OnnxClip => "Clip",
    OnnxSign => "Sign",
    OnnxReciprocal => "Reciprocal",
    OnnxMod => "Mod",
    OnnxBitShift => "BitShift",
    OnnxBitwiseAnd => "BitwiseAnd",
    OnnxBitwiseOr => "BitwiseOr",
    OnnxBitwiseXor => "BitwiseXor",
    OnnxBitwiseNot => "BitwiseNot",
    OnnxMin => "Min",
    OnnxMax => "Max",
    OnnxMean => "Mean",
    OnnxSum => "Sum",

    // Trigonometric
    OnnxSin => "Sin",
    OnnxCos => "Cos",
    OnnxTan => "Tan",
    OnnxAsin => "Asin",
    OnnxAcos => "Acos",
    OnnxAtan => "Atan",
    OnnxSinh => "Sinh",
    OnnxCosh => "Cosh",
    OnnxAsinh => "Asinh",
    OnnxAcosh => "Acosh",
    OnnxAtanh => "Atanh",

    // Convolution and pooling
    OnnxConv => "Conv",
    OnnxConvTranspose => "ConvTranspose",
    OnnxConvInteger => "ConvInteger",
    OnnxAveragePool => "AveragePool",
    OnnxMaxPool => "MaxPool",
    OnnxGlobalAveragePool => "GlobalAveragePool",
    OnnxGlobalMaxPool => "GlobalMaxPool",
    OnnxGlobalLpPool => "GlobalLpPool",
    OnnxLpPool => "LpPool",
    OnnxMaxRoiPool => "MaxRoiPool",
    OnnxMaxUnpool => "MaxUnpool",

    // Normalization
    OnnxBatchNormalization => "BatchNormalization",
    OnnxInstanceNormalization => "InstanceNormalization",
    OnnxLayerNormalization => "LayerNormalization",
    OnnxGroupNormalization => "GroupNormalization",
    OnnxLpNormalization => "LpNormalization",
    OnnxMeanVarianceNormalization => "MeanVarianceNormalization",

    // Linear algebra
    OnnxMatMul => "MatMul",
    OnnxMatMulInteger => "MatMulInteger",
    OnnxGemm => "Gemm",

    // Tensor manipulation
    OnnxReshape => "Reshape",
    OnnxTranspose => "Transpose",
    OnnxFlatten => "Flatten",
    OnnxSqueeze => "Squeeze",
    OnnxUnsqueeze => "Unsqueeze",
    OnnxExpand => "Expand",
    OnnxTile => "Tile",
    OnnxPad => "Pad",
    OnnxSlice => "Slice",
    OnnxSplit => "Split",
    OnnxSplitToSequence => "SplitToSequence",
    OnnxConcat => "Concat",
    OnnxConcatFromSequence => "ConcatFromSequence",
    OnnxDepthToSpace => "DepthToSpace",
    OnnxSpaceToDepth => "SpaceToDepth",
    OnnxReverseSequence => "ReverseSequence",

    // Gather / Scatter
    OnnxGather => "Gather",
    OnnxGatherElements => "GatherElements",
    OnnxGatherND => "GatherND",
    OnnxScatter => "Scatter",
    OnnxScatterElements => "ScatterElements",
    OnnxScatterND => "ScatterND",

    // Reduction
    OnnxReduceMax => "ReduceMax",
    OnnxReduceMin => "ReduceMin",
    OnnxReduceMean => "ReduceMean",
    OnnxReduceSum => "ReduceSum",
    OnnxReduceProd => "ReduceProd",
    OnnxReduceL1 => "ReduceL1",
    OnnxReduceL2 => "ReduceL2",
    OnnxReduceLogSum => "ReduceLogSum",
    OnnxReduceLogSumExp => "ReduceLogSumExp",
    OnnxReduceSumSquare => "ReduceSumSquare",
    OnnxArgMax => "ArgMax",
    OnnxArgMin => "ArgMin",

    // Comparison and logic
    OnnxEqual => "Equal",
    OnnxGreater => "Greater",
    OnnxGreaterOrEqual => "GreaterOrEqual",
    OnnxLess => "Less",
    OnnxLessOrEqual => "LessOrEqual",
    OnnxNot => "Not",
    OnnxAnd => "And",
    OnnxOr => "Or",
    OnnxXor => "Xor",
    OnnxWhere => "Where",
    OnnxIsNaN => "IsNaN",
    OnnxIsInf => "IsInf",

    // Shape and type
    OnnxShape => "Shape",
    OnnxSize => "Size",
    OnnxCast => "Cast",
    OnnxCastLike => "CastLike",
    OnnxConstantOfShape => "ConstantOfShape",
    OnnxRange => "Range",
    OnnxOneHot => "OneHot",
    OnnxNonZero => "NonZero",
    OnnxTopK => "TopK",
    OnnxUnique => "Unique",
    OnnxEyeLike => "EyeLike",
    OnnxCompress => "Compress",

    // RNN
    OnnxLSTM => "LSTM",
    OnnxGRU => "GRU",
    OnnxRNN => "RNN",

    // Constants
    OnnxConstant => "Constant",
    OnnxIdentity => "Identity",

    // Control flow
    OnnxIf => "If",
    OnnxLoop => "Loop",
    OnnxScan => "Scan",

    // Resize
    OnnxResize => "Resize",
    OnnxUpsample => "Upsample",

    // Regularization
    OnnxDropout => "Dropout",

    // Quantization
    OnnxQuantizeLinear => "QuantizeLinear",
    OnnxDequantizeLinear => "DequantizeLinear",
    OnnxDynamicQuantizeLinear => "DynamicQuantizeLinear",
    OnnxQLinearConv => "QLinearConv",
    OnnxQLinearMatMul => "QLinearMatMul",

    // Attention / Transformer
    OnnxAttention => "Attention",
    OnnxEinsum => "Einsum",

    // Sequence
    OnnxSequenceConstruct => "SequenceConstruct",
    OnnxSequenceAt => "SequenceAt",
    OnnxSequenceEmpty => "SequenceEmpty",
    OnnxSequenceInsert => "SequenceInsert",
    OnnxSequenceErase => "SequenceErase",
    OnnxSequenceLength => "SequenceLength",
    OnnxSequenceMap => "SequenceMap",

    // Optional
    OnnxOptional => "Optional",
    OnnxOptionalGetElement => "OptionalGetElement",
    OnnxOptionalHasElement => "OptionalHasElement",

    // Image
    OnnxImageDecoder => "ImageDecoder",
    OnnxGridSample => "GridSample",
    OnnxRoiAlign => "RoiAlign",
    OnnxAffineGrid => "AffineGrid",

    // Misc
    OnnxCumSum => "CumSum",
    OnnxDet => "Det",
    OnnxErf => "Erf",
    OnnxHardmax => "Hardmax",
    OnnxShrink => "Shrink",
    OnnxStringNormalizer => "StringNormalizer",
    OnnxTfIdfVectorizer => "TfIdfVectorizer",
    OnnxNegativeLogLikelihoodLoss => "NegativeLogLikelihoodLoss",
    OnnxSoftmaxCrossEntropyLoss => "SoftmaxCrossEntropyLoss",
    OnnxBernoulli => "Bernoulli",
    OnnxRandomNormal => "RandomNormal",
    OnnxRandomNormalLike => "RandomNormalLike",
    OnnxRandomUniform => "RandomUniform",
    OnnxRandomUniformLike => "RandomUniformLike",
    OnnxMultinomial => "Multinomial",
    OnnxCenterCropPad => "CenterCropPad",
    OnnxCol2Im => "Col2Im",
    OnnxTrilu => "Trilu",
    OnnxBlackmanWindow => "BlackmanWindow",
    OnnxHannWindow => "HannWindow",
    OnnxHammingWindow => "HammingWindow",
    OnnxDFT => "DFT",
    OnnxMelWeightMatrix => "MelWeightMatrix",
    OnnxSTFT => "STFT",
    OnnxRegexFullMatch => "RegexFullMatch",
    OnnxStringConcat => "StringConcat",
    OnnxStringSplit => "StringSplit",
}

/// Build a Daft expression for an ONNX op by looking it up in the function registry.
pub fn onnx_op_to_expr(
    op_type: &onnx_rs::ast::OpType,
    inputs: Vec<ExprRef>,
) -> OnnxPlannerResult<ExprRef> {
    let op_name = op_type.as_str();
    let fn_name = format!("onnx.{op_name}");

    let registry = FUNCTION_REGISTRY.read().unwrap();
    let factory = registry.get(&fn_name).ok_or_else(|| OnnxPlannerError::UnsupportedOp {
        op_type: op_name.to_string(),
    })?;

    let args =
        FunctionArgs::new_unchecked(inputs.into_iter().map(FunctionArg::Unnamed).collect());

    let func = factory
        .get_function(args.clone(), &Schema::empty())
        .map_err(|e| OnnxPlannerError::DaftError { source: e })?;

    Ok(Expr::ScalarFn(ScalarFn::Builtin(BuiltinScalarFn {
        func,
        inputs: args,
    }))
    .arced())
}
