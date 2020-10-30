//! Contains importers and their traits.

pub mod graphml;

use crate::{workflow::StatusSender, Module, StepID};
use graphannis::update::GraphUpdate;
use std::{collections::BTreeMap, path::Path};

/// An importer is a module that takes a path and produces a list of graph update events.
/// Using the graph update event list allows to execute several importers in parallel and join them to a single annotation graph.
pub trait Importer: Module {
    /// Returns a list of graph update events for a single corpus.
    ///
    /// # Arguments
    ///
    /// * `input_path` - The path to the corpus files to import. Can be a single file or a directory. For directories, the importer should be able to find all relevant files in the directory.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn import_corpus(
        &self,
        input_path: &Path,
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
