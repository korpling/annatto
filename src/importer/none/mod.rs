use std::path::Path;

use super::Importer;
use crate::{progress::ProgressReporter, workflow::StatusSender, StepID};
use documented::{Documented, DocumentedFields};
use graphannis::update::GraphUpdate;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

/// A special importer that imports nothing.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use crate::importer::none::CreateEmptyCorpus;

    #[test]
    fn serialize() {
        let module = CreateEmptyCorpus::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }
}