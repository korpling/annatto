use std::{collections::BTreeMap, fs::File, path::Path};

use crate::{exporter::Exporter, progress::ProgressReporter, workflow::StatusSender, Module};

pub struct GraphMLExporter {}

impl Default for GraphMLExporter {
    fn default() -> Self {
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
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path), 1)?;
        let output_file = File::create(output_path)?;
        graphannis_core::graph::serialization::graphml::export(graph, Some("\n# configure visualizations here\n"), output_file, |msg| {
            reporter.info(msg).expect("Could not send status message");
        })?;
        reporter.worked(1)?;
        Ok(())
    }
}
