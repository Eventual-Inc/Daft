mod date;
mod day;
mod day_of_week;
mod month;
mod year;

use serde::{Deserialize, Serialize};

use crate::functions::temporal::{
    date::DateEvaluator, day::DayEvaluator, day_of_week::DayOfWeekEvaluator, month::MonthEvaluator,
    year::YearEvaluator,
};
use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TemporalExpr {
    Day,
    Month,
    Year,
    DayOfWeek,
    Date,
}

impl TemporalExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use TemporalExpr::*;
        match self {
            Day => &DayEvaluator {},
            Month => &MonthEvaluator {},
            Year => &YearEvaluator {},
            DayOfWeek => &DayOfWeekEvaluator {},
            Date => &DateEvaluator {},
        }
    }
}

pub fn date(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Date),
        inputs: vec![input.clone()],
    }
}

pub fn day(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Day),
        inputs: vec![input.clone()],
    }
}

pub fn month(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Month),
        inputs: vec![input.clone()],
    }
}

pub fn year(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Year),
        inputs: vec![input.clone()],
    }
}

pub fn day_of_week(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::DayOfWeek),
        inputs: vec![input.clone()],
    }
}
