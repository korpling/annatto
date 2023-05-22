//! Contains importers and their traits.

pub mod conllu;
pub mod corpus_annotations;
pub mod exmaralda;
pub mod graphml;
pub mod ptb;
pub mod spreadsheet;
pub mod textgrid;

use crate::{workflow::StatusSender, Module, StepID};
use graphannis::update::GraphUpdate;
use serde_derive::Deserialize;
use std::path::Path;

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
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;
}

pub const CREATE_EMPTY_CORPUS_MODULE_NAME: &str = "create_empty_corpus";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct CreateEmptyCorpus {}

impl Importer for CreateEmptyCorpus {
    fn import_corpus(
        &self,
        path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            let id = StepID {
                module_name: self.module_name().to_string(),
                path: Some(path.to_path_buf()),
            };
            tx.send(crate::workflow::StatusMessage::StepDone { id })?;
        }
        Ok(GraphUpdate::default())
    }
}

impl Module for CreateEmptyCorpus {
    fn module_name(&self) -> &str {
        CREATE_EMPTY_CORPUS_MODULE_NAME
    }
}
