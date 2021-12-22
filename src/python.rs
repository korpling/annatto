//! Run Python scripts as modules

use pyo3::{prelude::*, types::PyModule};
use rust_embed::RustEmbed;
use crate::{Module, importer::Importer};


#[derive(RustEmbed)]
#[folder = "py"]
struct Scripts;

pub struct PythonImporter {
    name: String,
    code: String,
}

impl Importer for PythonImporter {
    fn import_corpus(
        &self,
        _input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        Python::with_gil(|py| {
            let module =
                PyModule::from_code(py, &self.code, &format!("{}.py", self.name), &self.name)?;
        
            let result = module.getattr("start_import")?.call1(())?;
            dbg!(result);
            todo!()
        })
    }
}

impl Module for PythonImporter {
    fn module_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn run_dummy_importer() {
        let importer = PythonImporter {
            name: "DummyImporter".to_string(),
            code: String::from_utf8_lossy(&Scripts::get("DummyImporter.py").unwrap().data).to_string(),
        };
        let props = BTreeMap::default();
        let path = tempfile::NamedTempFile::new().unwrap();

        importer.import_corpus(path.path(), &props, None).unwrap();
    }
}
