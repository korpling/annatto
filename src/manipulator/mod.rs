use std::collections::BTreeMap;

use graphannis::AnnotationGraph;

use crate::Module;

pub trait Manipulator: Module {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
