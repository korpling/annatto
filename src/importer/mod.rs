//! Importer modules allow importing files from different formats.
pub mod conllu;
pub mod exmaralda;
pub mod file_nodes;
pub mod git;
pub mod graphml;
pub mod meta;
pub mod none;
pub mod opus;
pub mod paulaxml;
pub mod ptb;
pub mod relannis;
pub mod saltxml;
pub mod table;
pub mod text;
pub mod textgrid;
pub mod toolbox;
pub mod treetagger;
pub mod webanno;
pub mod whisper;
pub mod xlsx;
pub mod xml;

use crate::{StepID, workflow::StatusSender};
use graphannis::update::GraphUpdate;
use percent_encoding::{AsciiSet, CONTROLS};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// An importer is a module that takes a path and produces a list of graph update events.
/// Using the graph update event list allows to execute several importers in parallel and join them to a single annotation graph.
pub trait Importer: Sync {
    /// Returns a list of graph update events for a single corpus.
    ///
    /// # Arguments
    ///
    /// * `input_path` - The path to the corpus files to import. Can be a single file or a directory. For directories, the importer should be able to find all relevant files in the directory.
    /// * `step_id` - The ID of the step.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        config: GenericImportConfiguration,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;

    fn default_file_extensions(&self) -> &[&str];

    fn default_configuration(&self) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: None,
            extensions: self
                .default_file_extensions()
                .iter()
                .map(<&str>::to_string)
                .collect(),
        }
    }
}

/// An encoding set for node names.
///
/// This disallows `:` to avoid any possible ambiguities with the `::` annotation
/// match seperator. `/` disallowed so this separator can be used to build
/// hierarchical node IDs and simplifies using node names as file names.
/// Spaces ` ` are encoded to avoid problems with annotation names in the AQL syntax.
/// Since node names might be used as file names, all reserved charactes for
/// Windows file names are encoded as well.
pub const NODE_NAME_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b':')
    .add(b'/')
    .add(b' ')
    .add(b'%')
    .add(b'\\')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'|')
    .add(b'?')
    .add(b'*');

#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
pub struct GenericImportConfiguration {
    #[serde(alias = "as", default)]
    pub(crate) root_as: Option<String>,
    #[serde(default)]
    pub(crate) extensions: Vec<String>,
}

impl<'a> GenericImportConfiguration {
    pub fn custom_root_name(&'a self) -> Option<String> {
        self.root_as.clone()
    }

    pub fn extensions(&'a self) -> &'a Vec<String> {
        self.extensions.as_ref()
    }

    #[cfg(test)]
    pub fn new_with_root_name(root_name: String) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: Some(root_name),
            extensions: vec![],
        }
    }

    #[cfg(test)]
    pub fn new_with_extensions(extensions: Vec<String>) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: None,
            extensions,
        }
    }

    #[cfg(test)]
    pub fn new_with_default_extensions(importer: &dyn Importer) -> GenericImportConfiguration {
        use itertools::Itertools;

        GenericImportConfiguration {
            root_as: None,
            extensions: importer
                .default_file_extensions()
                .iter()
                .map(<&str>::to_string)
                .collect_vec(),
        }
    }

    #[cfg(test)]
    pub fn and_extensions(self, extensions: Vec<String>) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: self.root_as,
            extensions,
        }
    }
}
