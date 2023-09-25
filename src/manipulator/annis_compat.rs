use serde::Deserialize;

use crate::{progress::ProgressReporter, Module};

use super::Manipulator;

pub const MODULE_NAME: &str = "annis_compat";

/// Checks that the annotation graph complies with assumptions made be AQL/the ANNIS frontend and updates it when possible.
#[derive(Deserialize)]
pub struct AnnisCompatibility {}

impl Module for AnnisCompatibility {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Manipulator for AnnisCompatibility {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
