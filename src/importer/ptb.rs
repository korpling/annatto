use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

use encoding_rs_io::DecodeReaderBytes;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;
use pest::Parser;
use pest_derive::Parser;

use crate::{
    models::textgrid::TextGrid,
    progress::ProgressReporter,
    util::graphupdate::{path_structure, root_corpus_from_path},
    Module, Result,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_ptb";

#[derive(Parser)]
#[grammar = "importer/ptb/ptb.pest"]
pub struct PtbParser;

struct DocumentMapper<'a> {
    root_corpus: String,
    doc_path: String,
    text_node_name: String,
    reporter: &'a ProgressReporter,
    file_path: PathBuf,
}

impl<'a> DocumentMapper<'a> {
    fn map(&mut self, u: &mut GraphUpdate) -> Result<()> {
        // Add a subcorpus like node for the text
        u.add_event(UpdateEvent::AddNode {
            node_name: self.text_node_name.clone(),
            node_type: "datasource".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: self.text_node_name.clone(),
            target_node: self.doc_path.clone(),
            layer: ANNIS_NS.to_string(),
            component_type: "PartOf".to_string(),
            component_name: "".to_string(),
        })?;

        todo!()
    }
}

/// Importer the Penn Treebank Bracketed Text format (PTB)
#[derive(Default)]
pub struct PtbImporter {}

impl Module for PtbImporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for PtbImporter {
    fn import_corpus(
        &self,
        input_path: &Path,
        properties: &BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();

        let documents = path_structure(&mut u, input_path, &["ptb"])?;

        let reporter =
            ProgressReporter::new(tx, self as &dyn Module, Some(input_path), documents.len())?;

        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            let ptb = PtbParser::parse(Rule::ptb, &file_content)?;

            let text_node_name = format!("{}#text", &doc_path);
            let root_corpus = root_corpus_from_path(input_path)?;

            let mut doc_mapper = DocumentMapper {
                root_corpus,
                doc_path,
                reporter: &reporter,
                file_path,
                text_node_name,
            };

            doc_mapper.map(&mut u)?;
            reporter.worked(1)?;
        }

        todo!()
    }
}
