use std::collections::HashMap;

use graphannis::AnnotationGraph;

use crate::Module;

pub trait Manipulator: Module {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
