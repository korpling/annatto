use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::StepID;
use documented::{Documented, DocumentedFields};

use super::Manipulator;

/// A graph operation that does nothing.
/// The purpose of this graph operation is to allow to omit a `format` field in
/// the `[[graph_op]]` configuration of the workflow file.
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
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
