use crate::{workflow::StatusSender, Module, StepID};
use graphannis::update::GraphUpdate;
use std::{collections::BTreeMap, path::Path};

pub mod graphml;

pub trait Importer: Module {
    fn import_corpus(
        &self,
        path: &Path,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;
}

pub struct DoNothingImporter {}

impl DoNothingImporter {
    pub fn new() -> DoNothingImporter {
        DoNothingImporter {}
    }
}

impl Importer for DoNothingImporter {
    fn import_corpus(
        &self,
        path: &Path,
        _properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            let id = StepID {
                module_name: self.module_name().to_string(),
                path: Some(path.to_path_buf()),
            };
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(GraphUpdate::default())
    }
}

impl Module for DoNothingImporter {
    fn module_name(&self) -> &str {
        "DoNothingImporter"
    }
}
