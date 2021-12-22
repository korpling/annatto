//! Run Python scripts as modules

mod graph;

use crate::{importer::Importer, Module};
use pyo3::{prelude::*, types::PyModule, wrap_pymodule};
use rust_embed::RustEmbed;

use self::graph::GraphUpdate;

#[derive(RustEmbed)]
#[folder = "py"]
struct Scripts;

#[pymodule]
pub fn graph(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<GraphUpdate>()?;
    // Automatically add this module to the execution environment
    // https://github.com/PyO3/pyo3/issues/759#issuecomment-977835119
    py.import("sys")?.getattr("modules")?.set_item("graph", m)?;

    Ok(())
}

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
            wrap_pymodule!(graph)(py);

            let code_module = PyModule::from_code(py, &self.code, "", "")?;

            let result = code_module.getattr("start_import")?.call1(())?;
            dbg!(result);
            Ok(graphannis::update::GraphUpdate::default())
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
            code: String::from_utf8_lossy(&Scripts::get("DummyImporter.py").unwrap().data)
                .to_string(),
        };
        let props = BTreeMap::default();
        let path = tempfile::NamedTempFile::new().unwrap();

        importer.import_corpus(path.path(), &props, None).unwrap();
    }
}
