use std::{collections::BTreeSet, fs, path::Path};

use facet::Facet;
use graphannis::update::GraphUpdate;
use serde::{Deserialize, Serialize};

use crate::importer::Importer;

/// Import annotations provided in the fieldlinguist's toolbox text format.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FLToolbox {
    /// This attribute sets the annotation layer, that other annotations will point to.
    /// This needs to be set to avoid an invalid model.
    target: String,
    /// The annotation names named here are considered single-valued per line. Space values
    /// are not considered delimiters, but part of the annotation value. Such annotations
    /// rely on the existence of the target nodes, i. e. annotation lines without any other
    /// non-spanning annotation in the block will be dropped.
    #[serde(default)]
    span: BTreeSet<String>,
    /// Null values are represented as `-` in toolbox. If you want those to remain explicit
    /// annotations, set `explicit_null = true`.
    #[serde(default)]
    explicit_null: bool,
}

impl Importer for FLToolbox {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let named_paths = config.derive_corpus_graph(input_path, &mut update)?;

        Ok(update)
    }

    fn default_file_extensions(&self) -> &[&str] {
        &FLToolbox::DEFAULT_FILE_EXTENSIONS
    }
}

impl FLToolbox {
    pub(crate) const DEFAULT_FILE_EXTENSIONS: [&str; 1] = ["txt"];

    fn import_document(&self, path: &Path, doc_node_name: &str, update: &mut GraphUpdate) -> crate::error::Result<()> {
        let content = fs::read_to_string(path);  // TODO buffer to not crash on large files
        
        Ok(())
    }
}
