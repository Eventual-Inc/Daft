//! This module contains rex (expression dsl) serde.

use crate::{proto::Proto, rex::Expr};

impl Proto for Expr {
    type Message;

    fn from_proto(message: Self::Message) -> common_error::DaftResult<Self>
    where
        Self: Sized {
        todo!()
    }

    fn to_proto(&self) -> common_error::DaftResult<Self::Message> {
        todo!()
    }
}
