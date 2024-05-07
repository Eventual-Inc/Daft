mod capitalize;
mod contains;
mod endswith;
mod extract;
mod extract_all;
mod find;
mod left;
mod length;
mod lower;
mod lpad;
mod lstrip;
mod match_;
mod repeat;
mod replace;
mod reverse;
mod right;
mod rpad;
mod rstrip;
mod split;
mod startswith;
mod upper;

use capitalize::CapitalizeEvaluator;
use contains::ContainsEvaluator;
use endswith::EndswithEvaluator;
use extract::ExtractEvaluator;
use extract_all::ExtractAllEvaluator;
use find::FindEvaluator;
use left::LeftEvaluator;
use length::LengthEvaluator;
use lower::LowerEvaluator;
use lpad::LpadEvaluator;
use lstrip::LstripEvaluator;
use repeat::RepeatEvaluator;
use replace::ReplaceEvaluator;
use reverse::ReverseEvaluator;
use right::RightEvaluator;
use rpad::RpadEvaluator;
use rstrip::RstripEvaluator;
use serde::{Deserialize, Serialize};
use split::SplitEvaluator;
use startswith::StartswithEvaluator;
use upper::UpperEvaluator;

use crate::{functions::utf8::match_::MatchEvaluator, Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Utf8Expr {
    EndsWith,
    StartsWith,
    Contains,
    Split(bool),
    Match,
    Extract(usize),
    ExtractAll(usize),
    Replace(bool),
    Length,
    Lower,
    Upper,
    Lstrip,
    Rstrip,
    Reverse,
    Capitalize,
    Left,
    Right,
    Find,
    Rpad,
    Lpad,
    Repeat,
}

impl Utf8Expr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use Utf8Expr::*;
        match self {
            EndsWith => &EndswithEvaluator {},
            StartsWith => &StartswithEvaluator {},
            Contains => &ContainsEvaluator {},
            Split(_) => &SplitEvaluator {},
            Match => &MatchEvaluator {},
            Extract(_) => &ExtractEvaluator {},
            ExtractAll(_) => &ExtractAllEvaluator {},
            Replace(_) => &ReplaceEvaluator {},
            Length => &LengthEvaluator {},
            Lower => &LowerEvaluator {},
            Upper => &UpperEvaluator {},
            Lstrip => &LstripEvaluator {},
            Rstrip => &RstripEvaluator {},
            Reverse => &ReverseEvaluator {},
            Capitalize => &CapitalizeEvaluator {},
            Left => &LeftEvaluator {},
            Right => &RightEvaluator {},
            Find => &FindEvaluator {},
            Rpad => &RpadEvaluator {},
            Lpad => &LpadEvaluator {},
            Repeat => &RepeatEvaluator {},
        }
    }
}

pub fn endswith(data: ExprRef, pattern: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::EndsWith),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn startswith(data: ExprRef, pattern: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::StartsWith),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn contains(data: ExprRef, pattern: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Contains),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn match_(data: ExprRef, pattern: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Match),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn split(data: ExprRef, pattern: ExprRef, regex: bool) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Split(regex)),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn extract(data: ExprRef, pattern: ExprRef, index: usize) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Extract(index)),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn extract_all(data: ExprRef, pattern: ExprRef, index: usize) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::ExtractAll(index)),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn replace(data: ExprRef, pattern: ExprRef, replacement: ExprRef, regex: bool) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Replace(regex)),
        inputs: vec![data, pattern, replacement],
    }
    .into()
}

pub fn length(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Length),
        inputs: vec![data],
    }
    .into()
}

pub fn lower(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Lower),
        inputs: vec![data],
    }
    .into()
}

pub fn upper(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Upper),
        inputs: vec![data],
    }
    .into()
}

pub fn lstrip(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Lstrip),
        inputs: vec![data],
    }
    .into()
}

pub fn rstrip(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Rstrip),
        inputs: vec![data],
    }
    .into()
}

pub fn reverse(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Reverse),
        inputs: vec![data],
    }
    .into()
}

pub fn capitalize(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Capitalize),
        inputs: vec![data],
    }
    .into()
}

pub fn left(data: ExprRef, count: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Left),
        inputs: vec![data, count],
    }
    .into()
}

pub fn right(data: ExprRef, count: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Right),
        inputs: vec![data, count],
    }
    .into()
}

pub fn find(data: ExprRef, pattern: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Find),
        inputs: vec![data, pattern],
    }
    .into()
}

pub fn rpad(data: ExprRef, length: ExprRef, pad: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Rpad),
        inputs: vec![data, length, pad],
    }
    .into()
}

pub fn lpad(data: ExprRef, length: ExprRef, pad: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Lpad),
        inputs: vec![data, length, pad],
    }
    .into()
}

pub fn repeat(data: ExprRef, count: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Repeat),
        inputs: vec![data, count],
    }
    .into()
}
