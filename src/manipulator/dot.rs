use super::Manipulator;
use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, StepID};
use documented::{Documented, DocumentedFields};
use graphannis_core::{
    annostorage::{Match, ValueSearch},
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::NodeID,
};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

/// Output the currrent graph as DOT for debugging it
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct DotDebug {}

impl Manipulator for DotDebug {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
