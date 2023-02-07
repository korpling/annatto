//! Run Python scripts as modules

mod graph;

use std::sync::Arc;

use crate::{error::AnnattoError, importer::Importer, Module};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyModule, PyTuple},
    wrap_pymodule,
};
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
        properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let python_interpreter = pyembed::MainPythonInterpreter::new(default_python_config())?;

        let u: PyResult<_> = python_interpreter.with_gil(|py| {
            // graphannis modul
            wrap_pymodule!(graphannis)(py);
            // graph update utils
            let code_source = &Scripts::get("graphupdate_util.py").unwrap().data;
            let util_code = &String::from_utf8_lossy(code_source)[..];
            PyModule::from_code(py, util_code, "_graphupdate_util.py", "graphupdate_util")?;
            // importer
            let code_module =
                PyModule::from_code(py, &self.code, &format!("{}.py", &self.name), &self.name)?;
            let args = PyTuple::new(py, [input_path.to_str()]);
            let result: graph::GraphUpdate = code_module
                .getattr("start_import")?
                .call(args, Some(properties.into_py_dict(py)))?
                .extract()?;

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

impl PythonImporter {
    pub fn from_name(name: &str) -> PythonImporter {
        let py_name = format!("{}.py", name);
        PythonImporter {
            name: name.to_string(),
            code: String::from_utf8_lossy(&Scripts::get(py_name.as_str()).unwrap().data)
                .to_string(),
        }
    }
}

impl Module for PythonImporter {
    fn module_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;
    use std::{collections::BTreeMap, env::temp_dir};

    use graphannis::{
        corpusstorage::{QueryLanguage, SearchQuery},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::annostorage::ValueSearch;
    use tempfile::tempdir_in;

    use super::*;

    #[test]
    fn run_dummy_importer() {
        let importer = PythonImporter::from_name("DummyImporter");
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

    #[test]
    fn run_exmaralda_importer() {
        let importer = PythonImporter::from_name("EXMARaLDAImporter");
        let props = BTreeMap::default();
        let path = Path::new("test/exmaralda/importer/");
        let mut u = importer.import_corpus(path, &props, None).unwrap();
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut u, |_| {}).unwrap();
        assert_eq!(1, 1)
    }

    #[test]
    fn run_conll_importer() {
        let importer = PythonImporter::from_name("CoNLLImporter");
        let props = BTreeMap::default();
        let path = Path::new("test/conll/importer/");
        let mut u = importer.import_corpus(path, &props, None).unwrap();
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut u, |_| {}).unwrap();
        assert_eq!(1, 1)
    }

    #[test]
    fn run_ptb_importer() {
        let importer = PythonImporter::from_name("PTBImporter");
        let props = BTreeMap::default();
        let path = Path::new("test/ptb/importer/");
        let mut u = importer.import_corpus(path, &props, None).unwrap();
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut u, |_| {}).unwrap();
        assert_eq!(1, 1)
    }
}
