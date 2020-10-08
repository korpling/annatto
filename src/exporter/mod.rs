use std::{collections::HashMap, path::Path};

use graphannis::AnnotationGraph;

use crate::Module;

pub trait Exporter: Module {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        properties: &HashMap<String, String>,
        output_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
