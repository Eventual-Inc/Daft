use std::fmt::{Display, Formatter};

use common_error::DaftError;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict, types::PyList};
use serde::{Deserialize, Serialize};

use crate::functions::python::RuntimePyObject;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Dependency {
    Package(String),
    Pip { pip: Vec<String> },
}

/// A struct used to define conda env configuration, refer to the documentation
/// https://docs.conda.io/projects/conda/en/stable/user-guide/tasks/manage-environments.html#create-env-file-manually
///
/// Currently we mainly support the core `name`, `channels` and `dependencies` configuration items,
/// which may be sufficient. If necessary, we will add support for more configuration items in the future.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Environment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<Dependency>>,
}

impl Display for Environment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_yaml::to_string(self) {
            Ok(yaml) => write!(f, "{}", yaml),
            Err(e) => write!(f, "Error serializing conda config to YAML: {}", e),
        }
    }
}

#[cfg(feature = "python")]
impl Environment {
    pub fn to_py_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let py_dict = PyDict::new(py);

        if let Some(name) = &self.name {
            py_dict.set_item("name", name)?;
        }

        if let Some(channels) = &self.channels {
            py_dict.set_item("channels", PyList::new(py, channels)?)?;
        }

        if let Some(deps) = &self.dependencies {
            let py_deps = PyList::new(py, &[] as &[Py<PyAny>])?;

            for dep in deps {
                match dep {
                    Dependency::Package(pkg) => py_deps.append(pkg)?,
                    Dependency::Pip { pip } => {
                        let pip_dict = PyDict::new(py);
                        pip_dict.set_item("pip", PyList::new(py, pip)?)?;
                        py_deps.append(pip_dict)?;
                    }
                }
            }

            py_dict.set_item("dependencies", py_deps)?;
        }

        Ok(py_dict)
    }

    pub fn from_py_dict(py_dict: &Bound<'_, PyDict>) -> PyResult<Self> {
        let name = py_dict
            .get_item("name")?
            .and_then(|v| v.extract::<String>().ok());
        let channels = py_dict
            .get_item("channels")?
            .and_then(|v| v.extract::<Vec<String>>().ok());

        let dependencies = if let Some(deps) = py_dict.get_item("dependencies")?.as_ref() {
            let py_list = deps.downcast::<PyList>()?;

            let mut vec = Vec::with_capacity(py_list.len());
            for elem in py_list.iter() {
                if let Ok(pkg_str) = elem.extract::<String>() {
                    vec.push(Dependency::Package(pkg_str));
                } else if let Ok(pkg_grp) = elem.downcast::<PyDict>() {
                    match pkg_grp.get_item("pip")? {
                        Some(pv) if pv.extract::<Vec<String>>().is_ok() => {
                            vec.push(Dependency::Pip { pip: pv.extract()? });
                        }
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid dependency group config: {}",
                                pkg_grp
                            )));
                        }
                    }
                } else {
                    return Err(PyValueError::new_err(format!(
                        "Unsupported conda dependencies type '{}'",
                        elem.get_type()
                    )));
                }
            }

            Some(vec)
        } else {
            None
        };

        Ok(Self {
            name,
            channels,
            dependencies,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Conda {
    EnvName(String),
    YamlFile(String),
    YamlConf(String),
}

/// A struct used to define UDF runtime env configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimeEnv {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conda: Option<Conda>,
}

impl RuntimeEnv {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut runtime_envs = vec![];
        if let Some(conda) = &self.conda {
            match conda {
                Conda::EnvName(value) | Conda::YamlFile(value) => {
                    runtime_envs.push(format!("conda = {value}"));
                }
                Conda::YamlConf(value) => {
                    let value = value
                        .lines()
                        .map(|line| format!("    {}", line))
                        .collect::<Vec<String>>()
                        .join("\n");
                    runtime_envs.push(format!("\n  conda = \n{value}\n"));
                }
            }
        }
        runtime_envs
    }
}

impl TryFrom<RuntimePyObject> for RuntimeEnv {
    type Error = DaftError;

    fn try_from(py_obj: RuntimePyObject) -> Result<Self, Self::Error> {
        #[cfg(feature = "python")]
        {
            Python::attach(|py| {
                let py_obj = py_obj.unwrap();
                let py_dict = py_obj.bind(py).downcast::<PyDict>()?;

                let conda = match py_dict.get_item("conda")? {
                    Some(conda) => {
                        if let Ok(conda_str) = conda.extract::<String>() {
                            if conda_str.to_lowercase().ends_with(".yml")
                                || conda_str.to_lowercase().ends_with(".yaml")
                            {
                                Some(Conda::YamlFile(conda_str))
                            } else {
                                Some(Conda::EnvName(conda_str))
                            }
                        } else if let Ok(conda_dict) = conda.downcast::<PyDict>() {
                            let env: Environment = Environment::from_py_dict(conda_dict)?;
                            Some(Conda::YamlConf(env.to_string()))
                        } else {
                            return Err(PyErr::new::<PyValueError, _>(format!(
                                "Parameter 'conda' in runtime_env must be either a string or dict, but got {}",
                                conda.get_type()
                            )));
                        }
                    }
                    None => None,
                };

                Ok(Self { conda })
            })
            .map_err(DaftError::PyO3Error)
        }
        #[cfg(not(feature = "python"))]
        {
            Ok(Self { conda: None })
        }
    }
}

#[cfg(feature = "python")]
impl TryFrom<RuntimeEnv> for Py<PyAny> {
    type Error = PyErr;

    fn try_from(rt_env: RuntimeEnv) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            let py_dict = PyDict::new(py);

            if let Some(conda) = rt_env.conda {
                match conda {
                    Conda::EnvName(value) | Conda::YamlFile(value) => {
                        py_dict.set_item("conda", value)?;
                    }
                    Conda::YamlConf(value) => {
                        let env: Environment = serde_yaml::from_str(&value).map_err(|e| {
                            PyErr::new::<PyValueError, _>(format!(
                                "Failed to parse yaml config to conda environment: {}",
                                e
                            ))
                        })?;

                        py_dict.set_item("conda", env.to_py_dict(py)?)?;
                    }
                }
            }

            Ok(py_dict.into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to check YAML roundtrip via serde_yaml
    fn serde_with_yaml(env: &Environment) -> Environment {
        let yaml = format!("{}", env);
        serde_yaml::from_str::<Environment>(&yaml)
            .expect("YAML should deserialize back to Environment")
    }

    #[test]
    fn test_environment_display() {
        // with no fields
        {
            let env = Environment {
                name: None,
                channels: None,
                dependencies: None,
            };
            assert_eq!("{}\n", format!("{}", env));
        }

        // with only name
        {
            let env = Environment {
                name: Some("test".to_string()),
                channels: None,
                dependencies: None,
            };
            assert_eq!(format!("{}", env), "name: test\n");
            assert_eq!(serde_with_yaml(&env), env);
        }

        // with only channels
        {
            let env = Environment {
                name: None,
                channels: Some(vec!["conda-forge".to_string()]),
                dependencies: None,
            };
            assert_eq!(format!("{}", env), "channels:\n- conda-forge\n");
            assert_eq!(serde_with_yaml(&env), env);
        }

        // with only dependencies
        {
            let env = Environment {
                name: None,
                channels: None,
                dependencies: Some(vec![
                    Dependency::Package("scipy==1.11.4".to_string()),
                    Dependency::Package("pip".to_string()),
                    Dependency::Pip {
                        pip: vec!["requests==2.31.0".to_string(), "httpx".to_string()],
                    },
                ]),
            };
            assert_eq!(
                format!("{}", env),
                "dependencies:\n- scipy==1.11.4\n- pip\n- pip:\n  - requests==2.31.0\n  - httpx\n"
            );
            assert_eq!(serde_with_yaml(&env), env);
        }

        // with all fields
        {
            let env = Environment {
                name: Some("test".to_string()),
                channels: Some(vec!["conda-forge".to_string(), "defaults".to_string()]),
                dependencies: Some(vec![
                    Dependency::Package("numpy=1.26".to_string()),
                    Dependency::Pip {
                        pip: vec!["pandas==2.0".to_string(), "urllib3".to_string()],
                    },
                ]),
            };
            assert_eq!(serde_with_yaml(&env), env);
        }
    }

    #[test]
    fn test_environment_multiline_display() {
        {
            let rt = RuntimeEnv {
                conda: Some(Conda::EnvName("prep".to_string())),
            };
            let lines = rt.multiline_display();
            assert_eq!(lines, vec!["conda = prep".to_string()]);
        }

        {
            let rt = RuntimeEnv {
                conda: Some(Conda::YamlFile("/path/to/env.yaml".to_string())),
            };
            let lines = rt.multiline_display();
            assert_eq!(lines, vec!["conda = /path/to/env.yaml".to_string()]);
        }

        {
            let yaml_conf = "name: test-env\nchannels:\n- conda-forge\ndependencies:\n- numpy\n- pip\n- pip:\n  - pandas\n".to_string();
            let rt = RuntimeEnv {
                conda: Some(Conda::YamlConf(yaml_conf.clone())),
            };
            let lines = rt.multiline_display();
            assert_eq!(lines.len(), 1);
            let expected = "\n  conda = \n    name: test-env\n    channels:\n    - conda-forge\n    dependencies:\n    - numpy\n    - pip\n    - pip:\n      - pandas\n";
            assert_eq!(lines[0], expected);
        }
    }

    #[test]
    fn test_try_from_runtime_py_object_without_python_feature() {
        let py_obj = RuntimePyObject::new_none();
        let rt: RuntimeEnv = py_obj
            .try_into()
            .expect("TryFrom should succeed without python");
        assert_eq!(rt.conda, None);
        assert!(rt.multiline_display().is_empty());
    }
}
