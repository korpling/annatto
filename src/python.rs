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

    use std::{collections::BTreeMap, env::temp_dir};
    use std::path::Path;

    use graphannis::corpusstorage::ResultOrder;
    use graphannis::{AnnotationGraph, CorpusStorage, corpusstorage::{SearchQuery, QueryLanguage}};
    use graphannis_core::annostorage::ValueSearch;
    use tempfile::tempdir_in;

    use crate::util::write_to_file;

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
    
    fn run_spreadsheet_import(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let importer = PythonImporter::from_name("import_spreadsheet");
        let mut props = BTreeMap::default();
        props.insert("column_map".to_string(), "dipl={sentence,seg};norm={pos,lemma}".to_string());
        let path = Path::new("./test/import/xlsx/");
        let import = importer.import_corpus(path, &props, None);
        let mut u = import?;
        write_to_file(&u, Path::new("xlsx_updates.log"))?;
        let mut g = AnnotationGraph::new(on_disk)?;
        g.apply_update(&mut u, |_| {})?;        
        let queries_and_results: [(&str, u64); 19] = [
            ("dipl", 4),
            ("norm", 4),
            ("dipl _=_ norm", 1),
            ("dipl _l_ norm", 3),
            ("dipl _r_ norm", 3),
            ("dipl:sentence", 1),
            ("dipl:seg", 2),
            ("dipl:sentence _=_ dipl", 0),
            ("dipl:sentence _o_ dipl", 4),
            ("dipl:sentence _l_ dipl", 1),
            ("dipl:sentence _r_ dipl", 1),
            ("dipl:seg _=_ dipl", 1),
            ("dipl:seg _o_ dipl", 4),
            ("dipl:seg _l_ dipl", 2),
            ("dipl:seg _r_ dipl", 2),            
            ("norm:pos", 4),
            ("norm:lemma", 4),
            ("norm:pos _=_ norm", 4),
            ("norm:lemma _=_ norm", 4)
        ];
        let corpus_name = "current";
        let tmp_dir = tempdir_in(temp_dir())?;
        g.save_to(&tmp_dir.path().join(corpus_name))?;
        let cs = CorpusStorage::with_auto_cache_size(&tmp_dir.path(), true).unwrap();
        for (query_s, expected_result) in queries_and_results {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let count = cs.count(query)?;
            assert_eq!(count, expected_result, "Result for query {} does not match", query_s);
        }
        Ok(())
    }

    #[test]
    fn spreadsheet_import_in_mem() {
        let import = run_spreadsheet_import(false);
        assert!(import.is_ok(), "Spreadsheet import failed with error: {:?}", import.err());
    }

    #[test]
    fn spreadsheet_import_on_disk() {        
        let import = run_spreadsheet_import(true);
        assert!(import.is_ok(), "Spreadsheet import failed with error: {:?}", import.err());
    }
}
