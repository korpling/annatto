use std::{io::Read, path::Path};

use crate::{
    progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files, Module, StepID,
};

use super::Importer;
use encoding_rs_io::DecodeReaderBytes;
use graphannis::update::GraphUpdate;
use pest::{iterators::Pairs, Parser};
use pest_derive::Parser;
use serde::Deserialize;

const FILE_ENDINGS: [&str; 5] = ["treetagger", "tab", "tt", "txt", "xml"];

pub const MODULE_NAME: &str = "import_textgrid";

#[derive(Parser)]
#[grammar = "importer/treetagger/treetagger.pest"]
pub struct TreeTaggerParser;

struct DocumentMapper {
    doc_path: String,
    text_node_name: String,
}

impl<'a> DocumentMapper {
    fn map(&mut self, _u: &mut GraphUpdate, mut _tt: Pairs<'a, Rule>) -> anyhow::Result<()> {
        todo!("Map single document")
    }
}

/// Importer for the file format used by the TreeTagger.
#[derive(Default, Deserialize)]
#[serde(default)]
pub struct TreeTaggerImporter {}

impl Module for TreeTaggerImporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for TreeTaggerImporter {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();

        let documents = import_corpus_graph_from_files(&mut u, input_path, &FILE_ENDINGS)?;

        let reporter = ProgressReporter::new(tx, step_id, documents.len())?;

        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            let ptb: Pairs<Rule> = TreeTaggerParser::parse(Rule::treetagger, &file_content)?;

            let text_node_name = format!("{}#text", &doc_path);

            let mut doc_mapper = DocumentMapper {
                doc_path,
                text_node_name,
            };

            doc_mapper.map(&mut u, ptb)?;
            reporter.worked(1)?;
        }
        Ok(u)
    }
}
