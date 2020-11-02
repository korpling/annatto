//! This module contains helper methods and structures to implement with legacy Java-based modules

use std::path::{Path, PathBuf};

use graphannis::update::{GraphUpdate, UpdateEvent};
use regex::Regex;
use walkdir::WalkDir;

use crate::error::PepperError;

/// Imports a corpus structure from a directory and returns a list of document file paths and their node name.
///
/// The root directory is modelled as the top-level corpus, each sub-directory is a sub-corpus and files matching the pattern are documents.
///
/// # Arguments
///
/// * `root_dir` - The root directory containing the corpus files.
/// * `file_pattern` - An optional regular expression which is applied to the file name and determines if the file is included or not. If `None`, all files are included.
/// * `updates` - A mutable reference to the graph update list. (Sub)- corpora and empty document nodes are added to the list, including all meta data.
pub fn import_corpus_structure(
    root_dir: &Path,
    file_pattern: Option<&str>,
    updates: &mut GraphUpdate,
) -> Result<Vec<(PathBuf, String)>, PepperError> {
    // Compile pattern as regular expression
    let file_pattern: Option<Regex> = if let Some(file_pattern) = file_pattern {
        Some(Regex::new(file_pattern)?)
    } else {
        None
    };

    let mut result = Vec::new();
    for e in WalkDir::new(root_dir) {
        let e = e?;

        //  For files, check if the file has the right extension/pattern
        if let Some(file_pattern) = &file_pattern {
            if e.file_type().is_file() && !file_pattern.is_match(&e.file_name().to_string_lossy()) {
                continue;
            }
        }

        let path_components: Vec<String> = e
            .path()
            .ancestors()
            .take(e.depth())
            .filter_map(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .collect();
        let node_name = path_components.join("/");

        // Add node itself
        updates.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "corpus".to_string(),
        })?;

        // Add connection to parent node if necessary
        if path_components.len() > 1 {
            let parent_name = path_components[0..=path_components.len() - 1].join("/");
            updates.add_event(UpdateEvent::AddEdge {
                source_node: node_name.clone(),
                target_node: parent_name,
                component_type: "PartOf".to_string(),
                layer: "".to_string(),
                component_name: "".to_string(),
            })?;
        }

        if e.file_type().is_file() {
            result.push((e.path().to_path_buf(), node_name));
        }
    }
    Ok(result)
}
