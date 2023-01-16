//! Contains manipulators and their traits.
pub mod check;
pub mod merge;
pub mod re;

use crate::{workflow::StatusSender, Module, StepID};
use graphannis::AnnotationGraph;
use std::collections::BTreeMap;

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
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DoNothingManipulator {}

impl Default for DoNothingManipulator {
    fn default() -> Self {
        DoNothingManipulator {}
    }
}

impl Manipulator for DoNothingManipulator {
    fn manipulate_corpus(
        &self,
        _graph: &mut graphannis::AnnotationGraph,
        _properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            let id = StepID {
                module_name: self.module_name().to_string(),
                path: None,
            };
            tx.send(crate::workflow::StatusMessage::Progress {
                id,
                total_work: 1,
                finished_work: 1,
            })?;
        }
        Ok(())
    }
}

impl Module for DoNothingManipulator {
    fn module_name(&self) -> &str {
        "DoNothingManipulator"
    }
}
