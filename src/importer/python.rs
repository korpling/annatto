//! Run Python scripts as importers

use graphannis::update::GraphUpdate;
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyModule},
};

use crate::Module;

use super::Importer;

pub struct PythonImporter {
    name: String,
}

impl Importer for PythonImporter {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        Python::with_gil(|py| {
            let graph_updates =
                PyModule::from_code(py, r#"ABC"#, &format!("{}.py", self.name), &self.name)?;
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
