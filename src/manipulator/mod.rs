//! Graph operation modules change the imported corpus data.
pub mod check;
pub mod chunker;
pub mod collapse;
pub mod enumerate;
pub mod link;
pub mod map;
pub mod merge;
pub mod no_op;
pub mod re;

use crate::{workflow::StatusSender, Module};
use graphannis::AnnotationGraph;
use std::path::Path;

/// A a manipulator is a module that changes an annotation graph.
/// Manipulators are applied in sequence to the same annotation graph instance.
pub trait Manipulator: Module {
    /// Manipulates an annotation graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - A mutable reference to the annotation graph to manipulate.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        workflow_directory: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
