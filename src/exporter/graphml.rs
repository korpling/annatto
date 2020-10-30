use std::{collections::BTreeMap, fs::File, path::Path};

use crate::{
    error::PepperError,
    exporter::Exporter,
    workflow::{
        StatusMessage::{self, Progress},
        StatusSender,
    },
    Module,
};

pub struct GraphMLExporter {}

impl GraphMLExporter {
    pub fn new() -> GraphMLExporter {
        GraphMLExporter {}
    }

    fn set_progress(
        &self,
        progress: f32,
        path: &Path,
        tx: &Option<StatusSender>,
    ) -> Result<(), PepperError> {
        if let Some(tx) = tx {
            tx.send(Progress {
                id: self.step_id(Some(path)),
                progress,
            })?;
        }
        Ok(())
    }
}

impl Module for GraphMLExporter {
    fn module_name(&self) -> &str {
        "GraphMLExporter"
    }
}

impl Exporter for GraphMLExporter {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        _properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.set_progress(0.0, output_path, &tx)?;
        let output_file = File::create(output_path)?;
        graphannis_core::graph::serialization::graphml::export(graph, None, output_file, |msg| {
            if let Some(ref tx) = tx {
                tx.send(StatusMessage::Info(msg.to_string()))
                    .expect("Could not send status message");
            }
        })?;
        self.set_progress(1.0, output_path, &tx)?;
        Ok(())
    }
}
