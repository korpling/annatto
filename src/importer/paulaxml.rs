use facet::Facet;
use graphannis::update::GraphUpdate;
use serde::{Deserialize, Serialize};

use crate::importer::{Importer, paulaxml::corpus_structure::CorpusMapper};

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
        _step_id: crate::StepID,
        _config: super::GenericImportConfiguration,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();
        let corpus_mapper = CorpusMapper::new();
        let path_to_node_name = corpus_mapper.map_corpus_structure(input_path, &mut updates)?;

        todo!()
    }

    fn default_file_extensions(&self) -> &[&str] {
        &[]
    }
}
