use std::{collections::HashMap, path::Path};

use graphannis::update::GraphUpdate;

use crate::Module;

pub trait Importer: Module {
    fn import_corpus(
        &self,
        path: &Path,
        properties: &HashMap<String, String>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;
}
