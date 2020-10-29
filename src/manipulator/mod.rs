use std::collections::BTreeMap;

use graphannis::AnnotationGraph;

use crate::{workflow::StatusSender, Module};

pub trait Manipulator: Module {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
