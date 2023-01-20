//! Contains exporters and their traits.

pub mod graphml;

use crate::{workflow::StatusSender, Module};
use graphannis::AnnotationGraph;
use std::{collections::BTreeMap, path::Path};

/// An exporter is a module that takes and existing annotation graph and writes out the content into the given path in a specific format.
pub trait Exporter: Module {
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
        properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[derive(Default)]
pub struct DoNothingExporter {}

impl Exporter for DoNothingExporter {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: self.step_id(Some(output_path)),
            })?;
        }
        Ok(())
    }
}

impl Module for DoNothingExporter {
    fn module_name(&self) -> &str {
        "DoNothingExporter"
    }
}
