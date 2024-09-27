use std::collections::HashMap;

use common_io_config::{HTTPConfig, S3Config};
use daft_core::prelude::{DataType, Field};
use daft_dsl::{literal_value, null_lit, Expr, ExprRef, LiteralValue};
use serde::{
    de::{DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use sqlparser::ast::{FunctionArg, FunctionArgOperator};

use super::SQLModule;
use crate::{
    functions::{SQLFunction, SQLFunctionArguments, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleConfig;

impl SQLModule for SQLModuleConfig {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("S3Config", S3ConfigFunction);
        parent.add_fn("HTTPConfig", HTTPConfigFunction);
    }
}

pub struct S3ConfigFunction;
macro_rules! item {
    ($name:expr, $ty:ident) => {
        (
            Field::new(stringify!($name), DataType::$ty),
            literal_value($name),
        )
    };
}
impl SQLFunction for S3ConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let deserializer = FunctionArgDeserializer::new(inputs, planner);
        let cfg = deserializer.deserialize::<S3Config>();
        println!("{:?}", cfg);
        let args: SQLFunctionArguments = planner.parse_function_args(
            inputs,
            &[
                "region_name",
                "endpoint_url",
                "key_id",
                "session_token",
                "access_key",
                "credentials_provider",
                "buffer_time",
                "max_connections_per_io_thread",
                "retry_initial_backoff_ms",
                "connect_timeout_ms",
                "read_timeout_ms",
                "num_tries",
                "retry_mode",
                "anonymous",
                "use_ssl",
                "verify_ssl",
                "check_hostname_ssl",
                "requester_pays",
                "force_virtual_addressing",
                "profile_name",
            ],
            0,
        )?;

        let region_name = args.try_get_named::<String>("region_name")?;
        let endpoint_url = args.try_get_named::<String>("endpoint_url")?;
        let key_id = args.try_get_named::<String>("key_id")?;
        let session_token = args.try_get_named::<String>("session_token")?;

        let access_key = args.try_get_named::<String>("access_key")?;
        let buffer_time = args.try_get_named("buffer_time")?.map(|t: i64| t as u64);

        let max_connections_per_io_thread = args
            .try_get_named("max_connections_per_io_thread")?
            .map(|t: i64| t as u32);

        let retry_initial_backoff_ms = args
            .try_get_named("retry_initial_backoff_ms")?
            .map(|t: i64| t as u64);

        let connect_timeout_ms = args
            .try_get_named("connect_timeout_ms")?
            .map(|t: i64| t as u64);

        let read_timeout_ms = args
            .try_get_named("read_timeout_ms")?
            .map(|t: i64| t as u64);

        let num_tries = args.try_get_named("num_tries")?.map(|t: i64| t as u32);
        let retry_mode = args.try_get_named::<String>("retry_mode")?;
        let anonymous = args.try_get_named::<bool>("anonymous")?;
        let use_ssl = args.try_get_named::<bool>("use_ssl")?;
        let verify_ssl = args.try_get_named::<bool>("verify_ssl")?;
        let check_hostname_ssl = args.try_get_named::<bool>("check_hostname_ssl")?;
        let requester_pays = args.try_get_named::<bool>("requester_pays")?;
        let force_virtual_addressing = args.try_get_named::<bool>("force_virtual_addressing")?;
        let profile_name = args.try_get_named::<String>("profile_name")?;

        let entries = vec![
            (Field::new("variant", DataType::Utf8), literal_value("s3")),
            item!(region_name, Utf8),
            item!(endpoint_url, Utf8),
            item!(key_id, Utf8),
            item!(session_token, Utf8),
            item!(access_key, Utf8),
            item!(buffer_time, UInt64),
            item!(max_connections_per_io_thread, UInt32),
            item!(retry_initial_backoff_ms, UInt64),
            item!(connect_timeout_ms, UInt64),
            item!(read_timeout_ms, UInt64),
            item!(num_tries, UInt32),
            item!(retry_mode, Utf8),
            item!(anonymous, Boolean),
            item!(use_ssl, Boolean),
            item!(verify_ssl, Boolean),
            item!(check_hostname_ssl, Boolean),
            item!(requester_pays, Boolean),
            item!(force_virtual_addressing, Boolean),
            item!(profile_name, Utf8),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
    }
}

pub struct HTTPConfigFunction;

impl SQLFunction for HTTPConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let deserializer = FunctionArgDeserializer::new(inputs, planner);
        let cfg = deserializer.deserialize::<HTTPConfig>();
        println!("cfg = {:?}", cfg);
        let args: SQLFunctionArguments =
            planner.parse_function_args(inputs, &["user_agent", "bearer_token"], 0)?;

        let user_agent = args.try_get_named::<String>("user_agent")?;
        let bearer_token = args.try_get_named::<String>("bearer_token")?;

        let entries = vec![
            (Field::new("variant", DataType::Utf8), literal_value("http")),
            item!(user_agent, Utf8),
            item!(bearer_token, Utf8),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
    }
}

struct FunctionArgDeserializer<'de> {
    inputs: &'de [sqlparser::ast::FunctionArg],
    planner: &'de crate::planner::SQLPlanner,
    index: usize,
}

impl<'de> FunctionArgDeserializer<'de> {
    fn new(
        inputs: &'de [sqlparser::ast::FunctionArg],
        planner: &'de crate::planner::SQLPlanner,
    ) -> Self {
        Self {
            inputs,
            planner,
            index: 0,
        }
    }

    fn deserialize<T: Deserialize<'de>>(self) -> Result<T, DeserializerError> {
        T::deserialize(self)
    }

    fn current_input(&self) -> Result<&sqlparser::ast::FunctionArg, DeserializerError> {
        if self.index < self.inputs.len() {
            Ok(&self.inputs[self.index])
        } else {
            Err(DeserializerError)
        }
    }
    fn next_input(&mut self) -> Result<&sqlparser::ast::FunctionArg, DeserializerError> {
        if self.index < self.inputs.len() {
            let input = &self.inputs[self.index];
            self.index += 1;
            Ok(input)
        } else {
            Err(DeserializerError)
        }
    }
}

#[derive(Debug)]
struct DeserializerError;

impl std::fmt::Display for DeserializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deserializer error")
    }
}
impl std::error::Error for DeserializerError {}
impl serde::de::Error for DeserializerError {
    fn custom<T: std::fmt::Display>(_msg: T) -> Self {
        DeserializerError
    }
}
impl<'de> serde::de::Deserializer<'de> for FunctionArgDeserializer<'de> {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let mut positional_args = HashMap::new();
        let mut named_args = HashMap::new();
        for (idx, arg) in self.inputs.iter().enumerate() {
            match arg {
                FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::Assignment,
                } => {
                    if !fields.contains(&name.value.as_str()) {
                        todo!()
                    }
                    named_args.insert(
                        name.to_string(),
                        self.planner.try_unwrap_function_arg_expr(&arg).unwrap(),
                    );
                }
                FunctionArg::Unnamed(arg) => {
                    todo!()
                }
                _ => todo!(),
            }
        }
        // for field in fields {
        //     if !named_args.contains_key(*field) {
        //         named_args.insert(field.to_string(), null_lit());
        //     }
        // }

        let res = visitor.visit_map(StructDeserializer {
            fields,
            named_args,
            positional_args,
            field_index: 0,
        });
        res.map_err(|e| e.into())
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}

struct StructDeserializer {
    fields: &'static [&'static str],
    named_args: HashMap<String, ExprRef>,
    positional_args: HashMap<usize, ExprRef>,
    field_index: usize,
}

impl<'de, 'a> MapAccess<'de> for StructDeserializer {
    type Error = DeserializerError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.field_index >= self.fields.len() {
            Ok(None)
        } else {
            let field = self.fields[self.field_index];
            seed.deserialize(field.into_deserializer()).map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let field = self.fields[self.field_index];
        self.field_index += 1;

        if let Some(value) = self.named_args.get(field) {
            let value = value.as_literal().unwrap().clone();

            seed.deserialize(LiteralValueDeserializer { value: Some(value) })

            // Handle named argument
        } else if let Some(value) = self.positional_args.get(&(self.field_index - 1)) {
            // Handle positional argument
            todo!()
        } else {
            seed.deserialize(LiteralValueDeserializer { value: None })
        }
    }
}

struct LiteralValueDeserializer {
    value: Option<LiteralValue>,
}

impl<'de> Deserializer<'de> for LiteralValueDeserializer {
    type Error = DeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.value.unwrap().as_bool().unwrap())
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.value.unwrap().as_i32().unwrap() as i8)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.value.unwrap().as_i32().unwrap() as i16)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.value.unwrap().as_i32().unwrap())
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.value.unwrap().as_i64().unwrap())
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.value.unwrap().as_u32().unwrap() as u8)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.value.unwrap().as_u32().unwrap() as u16)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.value.unwrap().as_u32().unwrap())
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.value.unwrap().as_u64().unwrap())
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.value.unwrap().as_f64().unwrap() as f32)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.value.unwrap().as_f64().unwrap())
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.value.unwrap().as_str().unwrap_or(""))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            None => visitor.visit_str(""),
            Some(LiteralValue::Null) => visitor.visit_none(),
            Some(LiteralValue::Utf8(s)) => visitor.visit_string(s),
            other => panic!("Expected string, got {:?}", other),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(self.value.unwrap().as_binary().unwrap())
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(self.value.unwrap().as_binary().unwrap().to_vec())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            None => visitor.visit_unit(),
            Some(LiteralValue::Null) => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}
