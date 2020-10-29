use std::{collections::BTreeMap, path::Path};

use graphannis::update::GraphUpdate;

use crate::manipulator::Manipulator;
use crate::Module;
use crate::{exporter::Exporter, StepID};
use crate::{importer::Importer, workflow::StatusSender};

pub struct DoNothingImporter {
    name: String,
}

impl DoNothingImporter {
    pub fn new() -> DoNothingImporter {
        DoNothingImporter {
            name: String::from("DoNothingImporter"),
        }
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
                module_name: self.name.to_string(),
                path: Some(path.to_path_buf()),
            };
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(GraphUpdate::default())
    }
}

impl Module for DoNothingImporter {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}
pub struct DoNothingManipulator {
    name: String,
}

impl DoNothingManipulator {
    pub fn new() -> DoNothingManipulator {
        DoNothingManipulator {
            name: String::from("DoNothingManipulator"),
        }
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
                module_name: self.name.to_string(),
                path: None,
            };
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(())
    }
}

impl Module for DoNothingManipulator {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}

pub struct DoNothingExporter {
    name: String,
}

impl DoNothingExporter {
    pub fn new() -> DoNothingExporter {
        DoNothingExporter {
            name: String::from("DoNothingExporter"),
        }
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
            let id = StepID {
                module_name: self.name.to_string(),
                path: Some(output_path.to_path_buf()),
            };
            tx.send(crate::workflow::StatusMessage::Progress { id, progress: 1.0 })?;
        }
        Ok(())
    }
}

impl Module for DoNothingExporter {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}
