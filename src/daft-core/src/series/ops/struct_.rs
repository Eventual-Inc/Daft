use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn struct_field(&self, name: &str) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Struct(_) => {
                let children = &self.struct_()?.children;

                for child in children {
                    if child.name() == name {
                        return Ok(child.clone());
                    }
                }

                Err(DaftError::FieldNotFound(format!(
                    "Field {} not found in schema: {:?}",
                    name,
                    children.iter().map(|c| c.name()).collect::<Vec<&str>>()
                )))
            }
            dt => Err(DaftError::TypeError(format!(
                "field not implemented for {}",
                dt
            ))),
        }
    }
}
