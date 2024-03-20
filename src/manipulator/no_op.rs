use serde_derive::Deserialize;

use crate::StepID;

use super::Manipulator;

#[derive(Deserialize, Default)]
pub struct NoOp {}

impl Manipulator for NoOp {
    fn manipulate_corpus(
        &self,
        _graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
