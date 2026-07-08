use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use facet::Facet;
use graphannis::update::GraphUpdate;
use roxmltree::{Document, Node, ParsingOptions};
use serde::{Deserialize, Serialize};

use crate::{
    importer::{
        Importer,
        paulaxml::{corpus_structure::CorpusMapper, document::DocumentMapper},
    },
    progress::ProgressReporter,
};

mod corpus_structure;
mod document;

/// Import a corpus in the stand-off PAULA XML format (<https://github.com/korpling/paula-xml>)
#[derive(Facet, Deserialize, Default, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportPaulaXml {}

impl Importer for ImportPaulaXml {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;

        let mut updates = GraphUpdate::new();
        let corpus_mapper = CorpusMapper::new();

        progress.info("Mapping PAULA XML corpus structure")?;
        let mapped_documents =
            corpus_mapper.map_corpus_structure(input_path, &config, &mut updates)?;

        let progress = ProgressReporter::new(tx, step_id, mapped_documents.len())?;

        // Map every document of the corpus separate
        for (doc_path, doc_node_name) in mapped_documents {
            DocumentMapper::read_document(&doc_path, &doc_node_name, &mut updates)?;
            progress.worked(1)?;
        }
        todo!()
    }

    fn default_file_extensions(&self) -> &[&str] {
        &[]
    }
}

/// Represents the PAULA XML files in a directory.
#[derive(Default)]
struct PaulaDirectory {
    /// Maps a file with the path to the raw XML content
    xml_by_path: HashMap<PathBuf, String>,
}

/// Represents the parsed XML files of a PAULA document.
/// Must be used in conjunction with [`PaulaDirectory`].
struct PaulaDocument<'input> {
    /// Maps a file with the given PAULA ID to the parsed XML content
    document_by_id: HashMap<String, Document<'input>>,
}

impl PaulaDirectory {
    fn open_directory<'a, P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut result = PaulaDirectory::default();
        // List all XML files in the directory
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(ext) = entry.path().extension()
                && (ext == "paula" || ext == "xml")
            {
                let content = std::fs::read_to_string(entry.path())?;
                result.xml_by_path.insert(entry.path(), content);
            }
        }

        Ok(result)
    }
}

impl<'input> PaulaDocument<'input> {
    fn from_directory(dir: &'input PaulaDirectory) -> Result<Self> {
        let mut result = PaulaDocument {
            document_by_id: HashMap::new(),
        };
        for (path, content) in &dir.xml_by_path {
            let mut parsing_options = ParsingOptions::default();
            parsing_options.allow_dtd = true;

            let doc = Document::parse_with_options(content, parsing_options)?;
            // Get the Paula ID from the header element
            let header = doc
                .root_element()
                .children()
                .filter(|n| n.tag_name().name() == "header")
                .next();
            let paula_id = if let Some(header) = header
                && let Some(attr) = header.attribute("paula_id")
            {
                attr.to_string()
            } else if let Some(file_stem) = path.file_stem() {
                // Use the file name as backup
                file_stem.to_string_lossy().to_string()
            } else {
                bail!("Invalid PAULA XML: no paula_id attribute in element <header>");
            };
            result.document_by_id.insert(paula_id, doc);
        }
        Ok(result)
    }

    fn by_paula_id(&self, id: &str) -> Option<&Document<'input>> {
        self.document_by_id.get(id)
    }

    /// Get all documents that have the given type in the header
    fn by_header_type(&self, paula_type: &str) -> Vec<&Document<'input>> {
        self.document_by_id
            .values()
            .filter(|d| {
                d.root_element().children().any(|n| {
                    n.is_element()
                        && n.has_tag_name("header")
                        && n.attribute("type")
                            .is_some_and(|v| v.to_lowercase() == paula_type)
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
