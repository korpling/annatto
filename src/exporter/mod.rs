use crate::{workflow::StatusSender, Module};
use graphannis::AnnotationGraph;
use std::{collections::BTreeMap, path::Path};

pub mod graphml;

pub trait Exporter: Module {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DoNothingExporter {}

impl DoNothingExporter {
    pub fn new() -> DoNothingExporter {
        DoNothingExporter {}
    }
}

impl Exporter for DoNothingExporter {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            tx.send(crate::workflow::StatusMessage::Progress {
                id: self.step_id(Some(output_path)),
                progress: 1.0,
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
