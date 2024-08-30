use crate::{error::Error, metadata::get_sort_order};

use super::{column_order::ColumnOrder, schema_descriptor::SchemaDescriptor, RowGroupMetaData};
use indexmap::IndexMap;
use parquet_format_safe::ColumnOrder as TColumnOrder;
use serde::{Deserialize, Serialize};

pub use crate::thrift_format::KeyValue;
mod key_value_metadata_serde {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct SerializableKeyValue {
        pub key: String,
        pub value: Option<String>,
    }
    impl From<KeyValue> for SerializableKeyValue {
        fn from(kv: KeyValue) -> Self {
            Self {
                key: kv.key,
                value: kv.value,
            }
        }
    }

    impl From<SerializableKeyValue> for KeyValue {
        fn from(kv: SerializableKeyValue) -> Self {
            Self {
                key: kv.key,
                value: kv.value,
            }
        }
    }

    pub fn serialize<S>(
        key_value_metadata: &Option<Vec<KeyValue>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match key_value_metadata {
            Some(key_value_metadata) => {
                let serializable_key_value_metadata: Vec<SerializableKeyValue> = key_value_metadata
                    .clone()
                    .into_iter()
                    .map(SerializableKeyValue::from)
                    .collect();
                serializer.serialize_some(&serializable_key_value_metadata)
            }
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<KeyValue>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serializable_key_value_metadata: Option<Vec<SerializableKeyValue>> =
            Option::deserialize(deserializer)?;
        match serializable_key_value_metadata {
            Some(serializable_key_value_metadata) => {
                let key_value_metadata: Vec<KeyValue> = serializable_key_value_metadata
                    .into_iter()
                    .map(KeyValue::from)
                    .collect();
                Ok(Some(key_value_metadata))
            }
            None => Ok(None),
        }
    }
}

pub type RowGroupList = IndexMap<usize, RowGroupMetaData>;

/// Metadata for a Parquet file.
// This is almost equal to [`parquet_format_safe::FileMetaData`] but contains the descriptors,
// which are crucial to deserialize pages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMetaData {
    /// version of this file.
    pub version: i32,
    /// number of rows in the file.
    pub num_rows: usize,
    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub created_by: Option<String>,
    /// The row groups of this file
    pub row_groups: RowGroupList,
    /// key_value_metadata of this file.
    #[serde(with = "key_value_metadata_serde")]
    pub key_value_metadata: Option<Vec<KeyValue>>,
    /// schema descriptor.
    pub schema_descr: SchemaDescriptor,
    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None` is returned, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub column_orders: Option<Vec<ColumnOrder>>,
}

impl FileMetaData {
    /// Returns the [`SchemaDescriptor`] that describes schema of this file.
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema_descr
    }

    /// returns the metadata
    pub fn key_value_metadata(&self) -> &Option<Vec<KeyValue>> {
        &self.key_value_metadata
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::Undefined)
    }

    /// Deserializes [`crate::thrift_format::FileMetaData`] into this struct
    pub fn try_from_thrift(metadata: parquet_format_safe::FileMetaData) -> Result<Self, Error> {
        let schema_descr = SchemaDescriptor::try_from_thrift(&metadata.schema)?;

        let row_groups_list = metadata
            .row_groups
            .into_iter()
            .map(|rg| RowGroupMetaData::try_from_thrift(&schema_descr, rg))
            .collect::<Result<Vec<RowGroupMetaData>, Error>>()?;

        let row_groups = RowGroupList::from_iter(row_groups_list.into_iter().enumerate());

        let column_orders = metadata
            .column_orders
            .map(|orders| parse_column_orders(&orders, &schema_descr));

        Ok(FileMetaData {
            version: metadata.version,
            num_rows: metadata.num_rows.try_into()?,
            created_by: metadata.created_by,
            row_groups,
            key_value_metadata: metadata.key_value_metadata,
            schema_descr,
            column_orders,
        })
    }

    /// Serializes itself to thrift's [`parquet_format_safe::FileMetaData`].
    pub fn into_thrift(self) -> parquet_format_safe::FileMetaData {
        parquet_format_safe::FileMetaData {
            version: self.version,
            schema: self.schema_descr.into_thrift(),
            num_rows: self.num_rows as i64,
            row_groups: self
                .row_groups
                .into_values()
                .map(|v| v.into_thrift())
                .collect(),
            key_value_metadata: self.key_value_metadata,
            created_by: self.created_by,
            column_orders: None, // todo
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        }
    }

    /// Clone this metadata and return a new one with the given row groups.
    pub fn clone_with_row_groups(&self, num_rows: usize, row_groups: RowGroupList) -> Self {
        Self {
            version: self.version,
            num_rows,
            created_by: self.created_by.clone(),
            row_groups,
            key_value_metadata: self.key_value_metadata.clone(),
            schema_descr: self.schema_descr.clone(),
            column_orders: self.column_orders.clone(),
        }
    }
}

/// Parses [`ColumnOrder`] from Thrift definition.
fn parse_column_orders(
    orders: &[TColumnOrder],
    schema_descr: &SchemaDescriptor,
) -> Vec<ColumnOrder> {
    schema_descr
        .columns()
        .iter()
        .zip(orders.iter())
        .map(|(column, order)| match order {
            TColumnOrder::TYPEORDER(_) => {
                let sort_order = get_sort_order(
                    &column.descriptor.primitive_type.logical_type,
                    &column.descriptor.primitive_type.converted_type,
                    &column.descriptor.primitive_type.physical_type,
                );
                ColumnOrder::TypeDefinedOrder(sort_order)
            }
        })
        .collect()
}
