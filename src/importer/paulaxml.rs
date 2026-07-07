use facet::Facet;
use graphannis::update::GraphUpdate;
use serde::{Deserialize, Serialize};

use crate::{
    importer::{
        Importer,
        paulaxml::{corpus_structure::CorpusMapper, document::DocumentMapper},
    },
    progress::ProgressReporter,
};

mod corpus_structure;
mod document;

/// Import a corpus in the stand-off PAULA XML format (<https://github.com/korpling/paula-xml>)
#[derive(Facet, Deserialize, Default, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportPaulaXml {}

impl Importer for ImportPaulaXml {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;

        let mut updates = GraphUpdate::new();
        let corpus_mapper = CorpusMapper::new();

        progress.info("Mapping PAULA XML corpus structure")?;
        let mapped_documents =
            corpus_mapper.map_corpus_structure(input_path, &config, &mut updates)?;

        let progress = ProgressReporter::new(tx, step_id, mapped_documents.len())?;

        // Map every document of the corpus separate
        for (doc_path, doc_node_name) in mapped_documents {
            DocumentMapper::read_document(&doc_path, &doc_node_name, &mut updates)?;
            progress.worked(1)?;
        }
        todo!()
    }

    fn default_file_extensions(&self) -> &[&str] {
        &[]
    }
}
