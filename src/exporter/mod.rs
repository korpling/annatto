use std::{collections::BTreeMap, path::Path};

use graphannis::AnnotationGraph;

use crate::Module;

pub trait Exporter: Module {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        properties: &BTreeMap<String, String>,
        output_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
