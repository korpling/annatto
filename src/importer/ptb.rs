use std::{collections::BTreeMap, path::Path, result};

use graphannis::update::GraphUpdate;

use crate::{progress::ProgressReporter, Module};

use super::Importer;

/// Importer the Penn Treebank Bracketed Text format (PTB)
#[derive(Default)]
pub struct PtbImporter {}

impl Module for PtbImporter {
    fn module_name(&self) -> &str {
        "PtbImporter"
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

        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(input_path), 1)?;

        todo!()
    }
}
