use std::{collections::BTreeMap, path::Path};

use graphannis::update::GraphUpdate;

use crate::{Module, importer::Importer, error::PepperError, workflow::StatusSender};

pub struct GraphMLImporter {}

impl GraphMLImporter {
    pub fn new() -> GraphMLImporter {
        GraphMLImporter {}
    }

    fn set_progress(&self, progress: f32, path: &Path, tx: &Option<StatusSender>) -> Result<(), PepperError> {
        if let Some(tx) = tx {
            tx.send(crate::workflow::StatusMessage::Progress {
                id: self.step_id(Some(path)),
                progress,
            })?;
        }
        Ok(())
    }
}

impl Importer for GraphMLImporter {
    fn import_corpus(
        &self,
        path: &Path,
        _properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        self.set_progress(0.0, path, &tx)?;

        // Load the GraphML files (could be a ZIP file, too) from the given location

        Ok(GraphUpdate::default())
    }
}

impl Module for GraphMLImporter {
    fn module_name(&self) -> &str {
        "GraphMLImporter"
    }
}
