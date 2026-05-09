use facet::Facet;
use serde::{Deserialize, Serialize};

use crate::importer::Importer;

mod corpus_structure;
mod document;

/// Import a corpus in the stand-off PAULA XML format (<https://github.com/korpling/paula-xml>)
#[derive(Facet, Deserialize, Default, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportPaulaXml {}

impl Importer for ImportPaulaXml {
    fn import_corpus(
        &self,
        _input_path: &std::path::Path,
        _step_id: crate::StepID,
        _config: super::GenericImportConfiguration,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        todo!()
    }

    fn default_file_extensions(&self) -> &[&str] {
        &[]
    }
}
