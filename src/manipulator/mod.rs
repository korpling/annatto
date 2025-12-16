//! Graph operation modules change the imported corpus data.
pub mod align;
pub mod check;
pub mod chunker;
pub mod collapse;
pub mod diff;
pub mod enumerate;
pub mod filter;
pub mod link;
pub mod map;
pub mod no_op;
pub mod re;
pub mod sleep;
pub mod split;
pub mod time;
pub mod visualize;
pub mod edit;

use crate::{
    StepID,
    workflow::{StatusMessage, StatusSender},
};
use graphannis::AnnotationGraph;
use std::path::Path;

/// A a manipulator is a module that changes an annotation graph.
/// Manipulators are applied in sequence to the same annotation graph instance.
pub trait Manipulator: Sync {
    /// Manipulates an annotation graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - A mutable reference to the annotation graph to manipulate.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `step_id` - The ID of the step.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        workflow_directory: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// If the manipulator queries the graph using AQL, the search requires graph statistics.
    fn requires_statistics(&self) -> bool;

    /// This step needs to be run before manipulation to make sure the graph has updated statistics,
    /// given the module indicates the requirement via [requires_statistics()].
    fn validate_graph(
        &self, // this is only because Rust doesn't allow associated functions for trait objects while being dyn compatible
        graph: &mut AnnotationGraph,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), anyhow::Error> {
        if self.requires_statistics() && graph.global_statistics.is_none() {
            if let Some(sender) = tx {
                sender.send(StatusMessage::Info(format!(
                    "Computing graph statistics for step {} ...",
                    step_id.module_name
                )))?;
            }
            graph.calculate_all_statistics()?;
        }
        Ok(())
    }
}
