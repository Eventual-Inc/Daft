use std::collections::HashMap;

use azure_core::{
    Error,
    error::{ErrorKind, ResultExt},
};

#[derive(Debug, Clone)]
pub(crate) enum Env {
    Process(ProcessEnv),
    Mem(MemEnv),
}

impl Default for Env {
    fn default() -> Self {
        Self::Process(ProcessEnv)
    }
}

impl Env {
    pub fn var(&self, key: &str) -> azure_core::Result<String> {
        match self {
            Env::Process(env) => env.var(key),
            Env::Mem(env) => env.var(key),
        }
    }
}

impl From<ProcessEnv> for Env {
    fn from(env: ProcessEnv) -> Self {
        Self::Process(env)
    }
}

impl From<MemEnv> for Env {
    fn from(env: MemEnv) -> Self {
        Self::Mem(env)
    }
}

/// The standard environment that gets variables from the process.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProcessEnv;

impl ProcessEnv {
    fn var(&self, key: &str) -> azure_core::Result<String> {
        std::env::var(key).with_context(ErrorKind::Io, || {
            format!("environment variable {} not set", key)
        })
    }
}

/// An environment that stores and gets variables in memory.
#[derive(Debug, Clone, Default)]
pub(crate) struct MemEnv {
    vars: HashMap<String, String>,
}

impl MemEnv {
    fn var(&self, key: &str) -> azure_core::Result<String> {
        self.vars.get(key).cloned().ok_or_else(|| {
            Error::message(
                ErrorKind::Io,
                format!("environment variable {} not set", key),
            )
        })
    }
}

impl From<&[(&str, &str)]> for Env {
    fn from(pairs: &[(&str, &str)]) -> Self {
        let mut vars = HashMap::new();
        for (k, v) in pairs {
            vars.insert(k.to_string(), v.to_string());
        }
        Self::Mem(MemEnv { vars })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var() {
        let env = Env::from(&[("CHRISTMAS_GRINCH", "You're a mean one, Mr. Grinch")][..]);
        assert_eq!(
            env.var("CHRISTMAS_GRINCH").unwrap(),
            "You're a mean one, Mr. Grinch"
        );
    }

    // test ProcessEnv::var() returns an error when the environment variable is not set
    #[test]
    fn test_env_var_not_set() {
        let env = ProcessEnv {};
        assert!(env.var("CHRISTMAS_GRINCH").is_err());
    }

    // test MemEnv::var() returns an error when the environment variable is not set
    #[test]
    fn test_mem_env_var_not_set() {
        let env = MemEnv::default();
        assert!(env.var("CHRISTMAS_GRINCH").is_err());
    }

    // test MemEnv::var() returns valid entries when multiple environment variables are set
    #[test]
    fn test_mem_env_var_multiple() {
        let env = Env::from(
            &[
                ("CHRISTMAS_GRINCH", "You're a mean one, Mr. Grinch"),
                ("CHRISTMAS_TREE", "O Christmas Tree, O Christmas Tree"),
                ("CHRISTMAS_SNOW", "Let it snow, let it snow, let it snow"),
            ][..],
        );
        assert_eq!(
            env.var("CHRISTMAS_GRINCH").unwrap(),
            "You're a mean one, Mr. Grinch"
        );
        assert_eq!(
            env.var("CHRISTMAS_TREE").unwrap(),
            "O Christmas Tree, O Christmas Tree"
        );
        assert_eq!(
            env.var("CHRISTMAS_SNOW").unwrap(),
            "Let it snow, let it snow, let it snow"
        );
    }
}
