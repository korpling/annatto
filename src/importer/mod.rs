use std::{collections::BTreeMap, path::Path};

use graphannis::update::GraphUpdate;

use crate::{workflow::StatusSender, Module};

pub trait Importer: Module {
    fn import_corpus(
        &self,
        path: &Path,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;
}
