//! Run Python scripts as importers

use graphannis::update::GraphUpdate;
use pyo3::{prelude::*, types::PyModule};

use crate::Module;

use super::Importer;

pub struct PythonImporter {
    name: String,
}

impl Importer for PythonImporter {
    fn import_corpus(
        &self,
        _input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        Python::with_gil(|py| {
            let graph_updates =
                PyModule::from_code(py, r#"print(__file__, __name__)"#, &format!("{}.py", self.name), &self.name)?;
            let mut tmp = GraphUpdate::default();
            Ok(tmp)
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
        };
        let props = BTreeMap::default();
        let path = tempfile::NamedTempFile::new().unwrap();

        importer.import_corpus(path.path(), &props, None).unwrap();
    }
}
