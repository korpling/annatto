use std::{collections::BTreeMap, fs::File, path::Path, io::Read};

use crate::{exporter::Exporter, progress::ProgressReporter, workflow::StatusSender, Module, error::AnnattoError};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS,NODE_TYPE_KEY,NODE_NAME_KEY}
};
use graphannis::model::{AnnotationComponentType,AnnotationComponent};
use itertools::Itertools;

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
        let file_name;
        if let Some(part_of_c) = graph.get_all_components(Some(AnnotationComponentType::PartOf), None).first() {
            let corpus_nodes = graph.get_node_annos().exact_anno_search(Some(NODE_TYPE_KEY.ns.as_str()), NODE_TYPE_KEY.name.as_str(), ValueSearch::Some("corpus"));
            let part_of_storage = graph.get_graphstorage(part_of_c).unwrap();
            let corpus_root = corpus_nodes.into_iter()
                                    .find(|n| part_of_storage.get_outgoing_edges((*n).as_ref().unwrap().node).count() == 0)
                                    .unwrap()?.node;
            file_name = format!("{}.graphml", graph.get_node_annos().get_value_for_item(&corpus_root, &NODE_NAME_KEY)?.unwrap());
        } else {
            let reason = String::from("Could not determine file name for graphML.");
            let err = AnnattoError::Export { reason: reason, exporter: self.module_name().to_string(), path: output_path.to_path_buf() };
            return Err(Box::new(err));
        }
        let output_file_path = match output_path.is_dir() {
            true => output_path.join(file_name),
            false => {
                let reason = String::from("File name provided instead of directory name.");
                let err = AnnattoError::Export { reason: reason, exporter: self.module_name().to_string(), path: output_path.to_path_buf() };
                return Err(Box::new(err));
            }
        };
        let output_file = File::create(output_file_path.clone())?;
        let vis_str = match properties.get(&PROPERTY_VISUALISATIONS.to_string()) {
            None => DEFAULT_VIS_STR,
            Some(visualisations) => visualisations
        };
        reporter.info(format!("Starting export to {}", &output_file_path.display()).as_str());
        graphannis_core::graph::serialization::graphml::export(graph, Some(format!("\n{}\n", vis_str).as_str()), output_file, |msg| {
            reporter.info(msg).expect("Could not send status message");
        })?;
        reporter.worked(1)?;
        Ok(())
    }
}
