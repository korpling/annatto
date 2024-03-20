use serde_derive::Deserialize;

use crate::Module;

use super::Manipulator;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NoOp {}

impl Module for NoOp {
    fn module_name(&self) -> &str {
        "NoOp"
    }
}

impl Manipulator for NoOp {
    fn manipulate_corpus(
        &self,
        _graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
