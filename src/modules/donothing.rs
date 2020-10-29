use std::{collections::BTreeMap, path::Path};

use graphannis::update::GraphUpdate;

use crate::manipulator::Manipulator;
use crate::Module;
use crate::{exporter::Exporter, StepID};
use crate::{importer::Importer, workflow::StatusSender};

pub struct DoNothingImporter {}

impl DoNothingImporter {
    pub fn new() -> DoNothingImporter {
        DoNothingImporter {}
    }
}

impl Importer for DoNothingImporter {
    fn import_corpus(
        &self,
        path: &Path,
        _properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            let id = StepID {
                module_name: self.module_name().to_string(),
                path: Some(path.to_path_buf()),
            };
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(GraphUpdate::default())
    }
}

impl Module for DoNothingImporter {
    fn module_name(&self) -> &str {
        "DoNothingImporter"
    }
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

pub struct DoNothingExporter {}

impl DoNothingExporter {
    pub fn new() -> DoNothingExporter {
        DoNothingExporter {}
    }
}

impl Exporter for DoNothingExporter {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = tx {
            tx.send(crate::workflow::StatusMessage::Progress {
                id: self.step_id(Some(output_path)),
                progress: 1.0,
            })?;
        }
        Ok(())
    }
}

impl Module for DoNothingExporter {
    fn module_name(&self) -> &str {
        "DoNothingExporter"
    }
}
