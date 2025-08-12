//! Exporter modules export the data into different formats.

pub mod conllu;
pub mod exmaralda;
pub mod graphml;
pub(crate) mod meta;
pub mod saltxml;
pub mod sequence;
pub(crate) mod table;
pub mod textgrid;
pub mod treetagger;
pub mod xlsx;

use crate::{StepID, workflow::StatusSender};
use graphannis::AnnotationGraph;
use std::path::Path;

/// An exporter is a module that takes and existing annotation graph and writes out the content into the given path in a specific format.
pub trait Exporter: Sync {
    /// Export an annotation graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - A reference to the annotation graph to export.
    /// * `output_path` - The path where to save the corpus files to. Can be a single file or a directory depending on the format.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        output_path: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn file_extension(&self) -> &str;
}
