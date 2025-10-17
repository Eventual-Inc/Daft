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
            let py_deps = PyList::new(py, &[] as &[PyObject])?;

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
            Python::with_gil(|py| {
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
impl TryFrom<RuntimeEnv> for PyObject {
    type Error = PyErr;

    fn try_from(rt_env: RuntimeEnv) -> Result<Self, Self::Error> {
        Python::with_gil(|py| {
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
