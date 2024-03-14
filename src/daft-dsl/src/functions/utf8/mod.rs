mod capitalize;
mod contains;
mod endswith;
mod left;
mod extract;
mod length;
mod lower;
mod lstrip;
mod match_;
mod reverse;
mod rstrip;
mod split;
mod startswith;
mod upper;

use capitalize::CapitalizeEvaluator;
use contains::ContainsEvaluator;
use endswith::EndswithEvaluator;
use left::LeftEvaluator;
use extract::ExtractEvaluator;
use length::LengthEvaluator;
use lower::LowerEvaluator;
use lstrip::LstripEvaluator;
use reverse::ReverseEvaluator;
use rstrip::RstripEvaluator;
use serde::{Deserialize, Serialize};
use split::SplitEvaluator;
use startswith::StartswithEvaluator;
use upper::UpperEvaluator;

use crate::{functions::utf8::match_::MatchEvaluator, Expr};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Utf8Expr {
    EndsWith,
    StartsWith,
    Contains,
    Split,
    Match,
    Extract(i32),
    Length,
    Lower,
    Upper,
    Lstrip,
    Rstrip,
    Reverse,
    Capitalize,
    Left,
}

impl Utf8Expr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use Utf8Expr::*;
        match self {
            EndsWith => &EndswithEvaluator {},
            StartsWith => &StartswithEvaluator {},
            Contains => &ContainsEvaluator {},
            Split => &SplitEvaluator {},
            Match => &MatchEvaluator {},
            Extract(_) => &ExtractEvaluator {},
            Length => &LengthEvaluator {},
            Lower => &LowerEvaluator {},
            Upper => &UpperEvaluator {},
            Lstrip => &LstripEvaluator {},
            Rstrip => &RstripEvaluator {},
            Reverse => &ReverseEvaluator {},
            Capitalize => &CapitalizeEvaluator {},
            Left => &LeftEvaluator {},
        }
    }
}

pub fn endswith(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::EndsWith),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn startswith(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::StartsWith),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn contains(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Contains),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn match_(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Match),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn split(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Split),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn extract(data: &Expr, pattern: &Expr, index: i32) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Extract(index)),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn length(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Length),
        inputs: vec![data.clone()],
    }
}

pub fn lower(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Lower),
        inputs: vec![data.clone()],
    }
}

pub fn upper(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Upper),
        inputs: vec![data.clone()],
    }
}

pub fn lstrip(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Lstrip),
        inputs: vec![data.clone()],
    }
}

pub fn rstrip(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Rstrip),
        inputs: vec![data.clone()],
    }
}

pub fn reverse(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Reverse),
        inputs: vec![data.clone()],
    }
}

pub fn capitalize(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Capitalize),
        inputs: vec![data.clone()],
    }
}

pub fn left(data: &Expr, count: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Left),
        inputs: vec![data.clone(), count.clone()],
    }
}
