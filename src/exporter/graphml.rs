use std::{collections::BTreeMap, fs::File, path::Path, io::Read};

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


pub const PROPERTY_VISUALISATIONS: &str = "add.visualisations";
const DEFAULT_VIS_STR: &str = "# configure visualizations here";

impl Exporter for GraphMLExporter {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path), 1)?;
        let output_file = File::create(output_path)?;
        let vis_str = match properties.get(&PROPERTY_VISUALISATIONS.to_string()) {
            None => DEFAULT_VIS_STR,
            Some(visualisations) => visualisations
        };
        graphannis_core::graph::serialization::graphml::export(graph, Some(format!("\n{}\n", vis_str).as_str()), output_file, |msg| {
            reporter.info(msg).expect("Could not send status message");
        })?;
        reporter.worked(1)?;
        Ok(())
    }
}
