mod date;
mod day;
mod day_of_week;
mod hour;
mod minute;
mod month;
mod second;
mod time;
mod truncate;
mod year;

use serde::{Deserialize, Serialize};

use crate::functions::temporal::{
    date::DateEvaluator, day::DayEvaluator, day_of_week::DayOfWeekEvaluator, hour::HourEvaluator,
    minute::MinuteEvaluator, month::MonthEvaluator, second::SecondEvaluator, time::TimeEvaluator,
    truncate::TruncateEvaluator, year::YearEvaluator,
};
use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TemporalExpr {
    Day,
    Hour,
    Minute,
    Second,
    Month,
    Year,
    DayOfWeek,
    Date,
    Time,
    Truncate(String),
}

impl TemporalExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use TemporalExpr::*;
        match self {
            Day => &DayEvaluator {},
            Hour => &HourEvaluator {},
            Month => &MonthEvaluator {},
            Year => &YearEvaluator {},
            DayOfWeek => &DayOfWeekEvaluator {},
            Date => &DateEvaluator {},
            Minute => &MinuteEvaluator {},
            Second => &SecondEvaluator {},
            Time => &TimeEvaluator {},
            Truncate(..) => &TruncateEvaluator {},
        }
    }
}

pub fn date(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Date),
        inputs: vec![input],
    }
    .into()
}

pub fn day(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Day),
        inputs: vec![input],
    }
    .into()
}

pub fn hour(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Hour),
        inputs: vec![input],
    }
    .into()
}

pub fn minute(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Minute),
        inputs: vec![input],
    }
    .into()
}

pub fn second(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Second),
        inputs: vec![input],
    }
    .into()
}

pub fn time(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Time),
        inputs: vec![input],
    }
    .into()
}

pub fn month(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Month),
        inputs: vec![input],
    }
    .into()
}

pub fn year(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Year),
        inputs: vec![input],
    }
    .into()
}

pub fn day_of_week(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::DayOfWeek),
        inputs: vec![input],
    }
    .into()
}

pub fn truncate(input: ExprRef, freq: &str, relative_to: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Truncate(freq.to_string())),
        inputs: vec![input, relative_to],
    }
}
