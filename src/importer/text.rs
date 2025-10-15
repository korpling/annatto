use facet::Facet;
use serde::{Deserialize, Serialize};

use crate::importer::Importer;

mod tokenizer;

/// Importer for plain text files.
///
/// Example:
/// ```toml
/// [[import]]
/// format = "text"
/// path = "..."
///
/// [import.config]
/// ```
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportText {
    /// The encoding to use when for the input files. Defaults to UTF-8.
    #[serde(default)]
    file_encoding: Option<String>,
}

impl Importer for ImportText {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        todo!()
    }

    fn file_extensions(&self) -> &[&str] {
        &["txt"]
    }
}
