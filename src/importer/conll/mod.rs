use std::{collections::BTreeMap, convert::TryFrom};

use graphannis::update::GraphUpdate;
use itertools::Itertools;

use crate::Module;

use super::Importer;

pub const MODULE_NAME: &str = "import_conll";

pub struct ImportCoNLL {}

impl Default for ImportCoNLL {
    fn default() -> Self {
        Self {}
    }
}

impl Module for ImportCoNLL {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for ImportCoNLL {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();

        Ok(update)
    }
}

impl ImportCoNLL {
    fn import_document(
        &self,
        update: &mut GraphUpdate,
        corpus_path: &str,
        document_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
