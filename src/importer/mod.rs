//! Importer modules allow importing files from different formats.
pub mod conllu;
pub mod exmaralda;
pub mod file_nodes;
pub mod graphml;
pub mod meta;
pub mod none;
pub mod opus;
pub mod ptb;
pub mod relannis;
pub mod saltxml;
pub mod table;
pub mod textgrid;
pub mod toolbox;
pub mod treetagger;
pub mod webanno;
pub mod whisper;
pub mod xlsx;
pub mod xml;

use crate::{workflow::StatusSender, StepID};
use graphannis::update::GraphUpdate;
use percent_encoding::{AsciiSet, CONTROLS};
use std::path::Path;

/// An importer is a module that takes a path and produces a list of graph update events.
/// Using the graph update event list allows to execute several importers in parallel and join them to a single annotation graph.
pub trait Importer: Sync {
    /// Returns a list of graph update events for a single corpus.
    ///
    /// # Arguments
    ///
    /// * `input_path` - The path to the corpus files to import. Can be a single file or a directory. For directories, the importer should be able to find all relevant files in the directory.
    /// * `step_id` - The ID of the step.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;

    fn file_extensions(&self) -> &[&str];
}

/// An encoding set for node names.
///
/// This disallows `:` to avoid any possible ambiguities with the `::` annotation
/// match seperator. `/` disallowed so this separator can be used to build
/// hierarchical node IDs and simplifies using node names as file names.
/// Spaces ` ` are encoded to avoid problems with annotation names in the AQL syntax.
/// Since node names might be used as file names, all reserved charactes for
/// Windows file names are encoded as well.
pub const NODE_NAME_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b':')
    .add(b'/')
    .add(b' ')
    .add(b'%')
    .add(b'\\')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'|')
    .add(b'?')
    .add(b'*');
