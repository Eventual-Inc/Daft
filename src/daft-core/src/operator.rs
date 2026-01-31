use std::str::FromStr;

use common_error::DaftError;
use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(Display, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Operator {
    #[display("==")]
    Eq,
    #[display("<=>")]
    EqNullSafe,
    #[display("!=")]
    NotEq,
    #[display("<")]
    Lt,
    #[display("<=")]
    LtEq,
    #[display(">")]
    Gt,
    #[display(">=")]
    GtEq,
    #[display("+")]
    Plus,
    #[display("-")]
    Minus,
    #[display("*")]
    Multiply,
    #[display("/")]
    TrueDivide,
    #[display("//")]
    FloorDivide,
    #[display("%")]
    Modulus,
    #[display("&")]
    And,
    #[display("|")]
    Or,
    #[display("^")]
    Xor,
    #[display("<<")]
    ShiftLeft,
    #[display(">>")]
    ShiftRight,
}

impl Operator {
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::EqNullSafe
                | Self::NotEq
                | Self::Lt
                | Self::LtEq
                | Self::Gt
                | Self::GtEq
        )
    }

    pub fn is_equality(&self) -> bool {
        matches!(self, Self::Eq | Self::NotEq | Self::EqNullSafe)
    }

    pub fn is_ordering(&self) -> bool {
        matches!(self, Self::Lt | Self::LtEq | Self::Gt | Self::GtEq)
    }

    pub fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Xor)
    }

    pub fn is_arithmetic(&self) -> bool {
        !self.is_comparison() && !self.is_logical()
    }
}

impl FromStr for Operator {
    type Err = DaftError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "==" => Ok(Self::Eq),
            "<=>" => Ok(Self::EqNullSafe),
            "!=" => Ok(Self::NotEq),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::LtEq),
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::GtEq),
            "+" => Ok(Self::Plus),
            "-" => Ok(Self::Minus),
            "*" => Ok(Self::Multiply),
            "/" => Ok(Self::TrueDivide),
            "//" => Ok(Self::FloorDivide),
            "%" => Ok(Self::Modulus),
            "&" => Ok(Self::And),
            "|" => Ok(Self::Or),
            "^" => Ok(Self::Xor),
            "<<" => Ok(Self::ShiftLeft),
            ">>" => Ok(Self::ShiftRight),
            _ => Err(DaftError::ComputeError(format!("Invalid operator: {s}"))),
        }
    }
}
