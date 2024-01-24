use crate::Module;
use nlprule::tokenizer::chunk::Chunker as NlpRuleChunker;

use super::Manipulator;

pub struct Chunker {}

impl Module for Chunker {
    fn module_name(&self) -> &str {
        "Chunker"
    }
}

impl Manipulator for Chunker {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
