//! A special importer that imports nothing.
use std::path::Path;

use crate::{progress::ProgressReporter, workflow::StatusSender, StepID};
use graphannis::update::GraphUpdate;
use serde_derive::Deserialize;

use super::Importer;

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct CreateEmptyCorpus {}

impl Importer for CreateEmptyCorpus {
    fn import_corpus(
        &self,
        _path: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let progress_reporter = ProgressReporter::new(tx, step_id, 1)?;
        let graph_update = GraphUpdate::default();
        progress_reporter.worked(1)?;
        Ok(graph_update)
    }

    fn file_extensions(&self) -> &[&str] {
        &[]
    }
}
