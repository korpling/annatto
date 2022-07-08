//! Run Python scripts as modules

mod graph;

use std::sync::Arc;

use crate::{error::AnnattoError, importer::Importer, Module};
use pyo3::{prelude::*, types::PyModule, wrap_pymodule};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "py"]
struct Scripts;

/// Construct a graphannis module that is compatible with the graphANNIS Python API.
/// This allows us to use classes like the GraphUpdate in Annatto modules.
#[pymodule]
fn graphannis(py: Python, m: &PyModule) -> PyResult<()> {
    let graph_module = PyModule::new(py, "graph")?;
    graph_module.add_class::<graph::GraphUpdate>()?;
    m.add_submodule(graph_module)?;

    // Automatically add the parent graphannis module to the execution environment
    // https://github.com/PyO3/pyo3/issues/759#issuecomment-977835119
    py.import("sys")?
        .getattr("modules")?
        .set_item("graphannis", m)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("graphannis.graph", graph_module)?;

    Ok(())
}

include! {"../pyembedded/default_python_config.rs"}

pub struct PythonImporter {
    name: String,
    code: String,
}

impl Importer for PythonImporter {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let python_interpreter = pyembed::MainPythonInterpreter::new(default_python_config())?;

        let u: PyResult<_> = python_interpreter.with_gil(|py| {
            wrap_pymodule!(graphannis)(py);

            let code_module =
                PyModule::from_code(py, &self.code, &format!("{}.py", &self.name), &self.name)?;

            let result: graph::GraphUpdate =
                code_module.getattr("start_import")?.call1(())?.extract()?;

            Ok(result.u)
        });
        let u = u?;

        let result = Arc::try_unwrap(u)
            .map_err(|_| AnnattoError::Import {
                reason: "The Python object containing the import result had multiple owners."
                    .to_string(),
                importer: self.name.to_string(),
                path: input_path.to_path_buf(),
            })?
            .into_inner()?;
        Ok(result)
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

    use graphannis::AnnotationGraph;
    use graphannis_core::annostorage::ValueSearch;

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

        let mut u = importer.import_corpus(path.path(), &props, None).unwrap();
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut u, |_| {}).unwrap();

        // Test that the example graph has been created
        let token: Vec<_> = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Any)
            .collect();
        assert_eq!(5, token.len())
    }
}
