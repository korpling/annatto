use std::{collections::BTreeMap, fs::File, path::Path};

use crate::{exporter::Exporter, progress::ProgressReporter, workflow::StatusSender, Module};

pub struct GraphMLExporter {}

impl GraphMLExporter {
    pub fn new() -> GraphMLExporter {
        GraphMLExporter {}
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
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path));
        reporter.set_progress(0.0)?;
        let output_file = File::create(output_path)?;
        graphannis_core::graph::serialization::graphml::export(graph, None, output_file, |msg| {
            reporter.info(msg).expect("Could not send status message");
        })?;
        reporter.set_progress(1.0)?;
        Ok(())
    }
}
