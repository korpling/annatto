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


pub const PROPERTY_VISUALISATION_PATH: &str = "load.visualisations";
const DEFAULT_VIS_STR: &str = "\n# configure visualizations here\n";

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
        let vis_str = match properties.get(&PROPERTY_VISUALISATION_PATH.to_string()) {
            None => DEFAULT_VIS_STR,
            Some(vis_path) => {
                let vis_file = File::open(Path::from(vis_path))?;
                let mut vis = String::new();
                let vis_data = vis_file.read_to_string(&mut buf)?;
                vis.as_str()
            }
        };
        graphannis_core::graph::serialization::graphml::export(graph, Some(vis_str), output_file, |msg| {
            reporter.info(msg).expect("Could not send status message");
        })?;
        reporter.worked(1)?;
        Ok(())
    }
}
