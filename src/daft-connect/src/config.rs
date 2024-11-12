use std::collections::BTreeMap;

use spark_connect::{
    config_request::{Get, GetAll, GetOption, GetWithDefault, IsModifiable, Set, Unset},
    ConfigResponse, KeyValue,
};
use tonic::Status;

use crate::Session;

impl Session {
    fn config_response(&self) -> ConfigResponse {
        ConfigResponse {
            session_id: self.client_side_session_id().to_string(),
            server_side_session_id: self.server_side_session_id().to_string(),
            pairs: vec![],
            warnings: vec![],
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn set(&mut self, operation: Set) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span =
            tracing::info_span!("set", session_id = %self.client_side_session_id(), ?operation);
        let _enter = span.enter();

        for KeyValue { key, value } in operation.pairs {
            let Some(value) = value else {
                let msg = format!("Missing value for key {key}. If you want to unset a value use the Unset operation");
                response.warnings.push(msg);
                continue;
            };

            let previous = self.config_values_mut().insert(key.clone(), value.clone());
            if previous.is_some() {
                tracing::info!("Updated existing configuration value");
            } else {
                tracing::info!("Set new configuration value");
            }
        }

        Ok(response)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn get(&self, operation: Get) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span = tracing::info_span!("get", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        for key in operation.keys {
            let value = self.config_values().get(&key).cloned();
            response.pairs.push(KeyValue { key, value });
        }

        Ok(response)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn get_with_default(&self, operation: GetWithDefault) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span =
            tracing::info_span!("get_with_default", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        for KeyValue {
            key,
            value: default_value,
        } in operation.pairs
        {
            let value = self.config_values().get(&key).cloned().or(default_value);
            response.pairs.push(KeyValue { key, value });
        }

        Ok(response)
    }

    /// Needs to be fixed so it has different behavior than [`Session::get`]. Not entirely
    /// sure how it should work yet.
    #[tracing::instrument(skip(self), level = "trace")]
    pub fn get_option(&self, operation: GetOption) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span = tracing::info_span!("get_option", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        for key in operation.keys {
            let value = self.config_values().get(&key).cloned();
            response.pairs.push(KeyValue { key, value });
        }

        Ok(response)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn get_all(&self, operation: GetAll) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span = tracing::info_span!("get_all", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        let Some(prefix) = operation.prefix else {
            for (key, value) in self.config_values() {
                response.pairs.push(KeyValue {
                    key: key.clone(),
                    value: Some(value.clone()),
                });
            }
            return Ok(response);
        };

        for (k, v) in prefix_search(self.config_values(), &prefix) {
            response.pairs.push(KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            });
        }

        Ok(response)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn unset(&mut self, operation: Unset) -> Result<ConfigResponse, Status> {
        let mut response = self.config_response();

        let span = tracing::info_span!("unset", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        for key in operation.keys {
            if self.config_values_mut().remove(&key).is_none() {
                let msg = format!("Key {key} not found");
                response.warnings.push(msg);
            } else {
                tracing::info!("Unset configuration value");
            }
        }

        Ok(response)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub fn is_modifiable(&self, _operation: IsModifiable) -> Result<ConfigResponse, Status> {
        let response = self.config_response();

        let span =
            tracing::info_span!("is_modifiable", session_id = %self.client_side_session_id());
        let _enter = span.enter();

        tracing::warn!(session_id = %self.client_side_session_id(), "is_modifiable operation not yet implemented");
        // todo: need to implement this
        Ok(response)
    }
}

fn prefix_search<'a, V>(
    map: &'a BTreeMap<String, V>,
    prefix: &'a str,
) -> impl Iterator<Item = (&'a String, &'a V)> {
    let start = map.range(prefix.to_string()..);
    start.take_while(move |(k, _)| k.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_prefix_search() {
        let mut map = BTreeMap::new();
        map.insert("apple".to_string(), 1);
        map.insert("application".to_string(), 2);
        map.insert("banana".to_string(), 3);
        map.insert("app".to_string(), 4);
        map.insert("apricot".to_string(), 5);

        // Test with prefix "app"
        let result: Vec<_> = prefix_search(&map, "app").collect();
        assert_eq!(
            result,
            vec![
                (&"app".to_string(), &4),
                (&"apple".to_string(), &1),
                (&"application".to_string(), &2),
            ]
        );

        // Test with prefix "b"
        let result: Vec<_> = prefix_search(&map, "b").collect();
        assert_eq!(result, vec![(&"banana".to_string(), &3),]);

        // Test with prefix that doesn't match any keys
        let result: Vec<_> = prefix_search(&map, "z").collect();
        assert_eq!(result, vec![]);

        // Test with empty prefix (should return all items)
        let result: Vec<_> = prefix_search(&map, "").collect();
        assert_eq!(
            result,
            vec![
                (&"app".to_string(), &4),
                (&"apple".to_string(), &1),
                (&"application".to_string(), &2),
                (&"apricot".to_string(), &5),
                (&"banana".to_string(), &3),
            ]
        );

        // Test with prefix that matches a complete key
        let result: Vec<_> = prefix_search(&map, "apple").collect();
        assert_eq!(result, vec![(&"apple".to_string(), &1),]);

        // Test with case sensitivity
        let result: Vec<_> = prefix_search(&map, "App").collect();
        assert_eq!(result, vec![]);
    }
}
