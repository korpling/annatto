use std::collections::BTreeMap;

use graphannis::AnnotationGraph;

use crate::{workflow::StatusSender, Module, StepID};

pub trait Manipulator: Module {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DoNothingManipulator {}

impl DoNothingManipulator {
    pub fn new() -> DoNothingManipulator {
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
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(())
    }
}

impl Module for DoNothingManipulator {
    fn module_name(&self) -> &str {
        "DoNothingManipulator"
    }
}
