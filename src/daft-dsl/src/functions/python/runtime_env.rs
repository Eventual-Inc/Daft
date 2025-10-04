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
impl TryFrom<&Bound<'_, PyDict>> for Environment {
    type Error = PyErr;

    fn try_from(py_dict: &Bound<'_, PyDict>) -> Result<Self, Self::Error> {
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
    YamlConf(Environment),
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
                Conda::EnvName(name) => runtime_envs.push(format!("conda = {name}")),
                Conda::YamlFile(file) => runtime_envs.push(format!("conda = {file}")),
                Conda::YamlConf(conf) => match serde_yaml::to_string(&conf) {
                    Ok(yaml) => {
                        let yaml = yaml
                            .lines()
                            .map(|line| format!("    {}", line))
                            .collect::<Vec<String>>()
                            .join("\n");
                        runtime_envs.push(format!("\n  conda = \n{yaml}\n"));
                    }
                    Err(err) => log::warn!("Failed to format conda env config: {}", err),
                },
            }
        }
        runtime_envs
    }
}

impl TryFrom<RuntimePyObject> for RuntimeEnv {
    type Error = DaftError;

    fn try_from(rt_py_obj: RuntimePyObject) -> Result<Self, Self::Error> {
        #[cfg(feature = "python")]
        {
            Python::with_gil(|py| {
                let py_obj = rt_py_obj.unwrap();
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
                            Some(Conda::YamlConf(conda_dict.try_into()?))
                        } else {
                            return Err(PyErr::new::<PyValueError, _>(
                               format!("Parameter 'conda' in runtime_env must be either a string or dict, but got {}", conda.get_type()),
                            ));
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
                    Conda::EnvName(name) | Conda::YamlFile(name) => {
                        py_dict.set_item("conda", name)?;
                    }
                    Conda::YamlConf(conf) => {
                        let conda_dict = PyDict::new(py);

                        if let Some(name) = conf.name {
                            conda_dict.set_item("name", name)?;
                        }

                        if let Some(channels) = conf.channels {
                            conda_dict.set_item("channels", PyList::new(py, channels)?)?;
                        }

                        if let Some(deps) = conf.dependencies {
                            let py_deps = PyList::new(py, &[] as &[Self])?;

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

                            conda_dict.set_item("dependencies", py_deps)?;
                        }

                        py_dict.set_item("conda", conda_dict)?;
                    }
                }
            }

            Ok(py_dict.into())
        })
    }
}
