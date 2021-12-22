//! This module contains helper methods and structures to implement with legacy Java-based modules
pub mod importer;
pub mod salt;

use std::{
    convert::TryFrom,
    io::Write,
    path::{Path, PathBuf},
};

use graphannis::update::{GraphUpdate, UpdateEvent};
use j4rs::{ClasspathEntry, Instance, InvocationArg, JavaOpt, Jvm, Null};
use regex::Regex;
use rust_embed::RustEmbed;
use tempfile::NamedTempFile;
use walkdir::WalkDir;

use crate::error::Result;

#[derive(RustEmbed)]
#[folder = "$OUT_DIR/pepper/plugins/"]
struct LegacyPluginFiles;

pub struct PepperPluginClasspath {
    files: Vec<NamedTempFile>,
}

impl PepperPluginClasspath {
    pub fn new() -> Result<PepperPluginClasspath> {
        // Get all plugin files and extract them to a temporary location
        let mut files = Vec::new();
        for jar_file in LegacyPluginFiles::iter() {
            let mut tmp_file = NamedTempFile::new()?;
            // Copy asset content to temporary file
            if let Some(content) = LegacyPluginFiles::get(&jar_file) {
                tmp_file.write_all(&content)?;
                files.push(tmp_file);
            }
        }
        Ok(PepperPluginClasspath { files })
    }

    pub fn create_jvm(&self, debug: bool) -> Result<Jvm> {
        let classpath_strings: Vec<_> = self
            .files
            .iter()
            .map(|f| f.path().to_string_lossy())
            .collect();

        let classpath_entries: Vec<_> = classpath_strings
            .iter()
            .map(|p| ClasspathEntry::new(p))
            .collect();
        let jvm = if debug {
            j4rs::JvmBuilder::new()
                .classpath_entries(classpath_entries)
                .java_opt(JavaOpt::new("-Xdebug"))
                .java_opt(JavaOpt::new(
                    "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5000",
                ))
                .build()?
        } else {
            j4rs::JvmBuilder::new()
                .classpath_entries(classpath_entries)
                .build()?
        };
        Ok(jvm)
    }
}

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
) -> Result<Vec<(PathBuf, String)>> {
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

        let mut path_components: Vec<String> = e
            .path()
            .ancestors()
            .take(e.depth() + 1)
            .filter_map(|p| p.file_stem())
            .map(|n| n.to_string_lossy().to_string())
            .collect();
        path_components.reverse();
        let node_name = path_components.join("/");

        // Add node itself
        updates.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "corpus".to_string(),
        })?;

        // Add connection to parent node if necessary
        if path_components.len() > 1 {
            let parent_name = path_components[0..path_components.len() - 1].join("/");
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

fn is_null(o: &Instance, jvm: &Jvm) -> Result<bool> {
    let result = jvm.invoke_static(
        "java.util.Objects",
        "equals",
        &[
            InvocationArg::try_from(Null::Of("java.lang.Object"))?,
            InvocationArg::from(jvm.chain(o)?.collect()),
        ],
    )?;
    Ok(jvm.to_rust(result)?)
}

fn is_instance_of(object: &Instance, class_name: &str, jvm: &Jvm) -> Result<bool> {
    let class_instance = jvm.invoke_static(
        "java.lang.Class",
        "forName",
        &[InvocationArg::try_from(class_name)?],
    )?;
    let object_arg = InvocationArg::from(jvm.cast(object, "java.lang.Object")?);
    let is_instance = jvm
        .chain(&class_instance)?
        .invoke("isInstance", &[object_arg])?
        .to_rust()?;
    Ok(is_instance)
}

fn prepare_mapper(
    mapper: &Instance,
    document: Instance,
    java_properties_class: &str,
    properties: &std::collections::BTreeMap<String, String>,
    jvm: &Jvm,
) -> Result<()> {
    // Create and set an empty property map
    let props = jvm.create_instance(java_properties_class, &[])?;

    // Add all property values for the mapper
    for (key, value) in properties {
        jvm.invoke(
            &props,
            "setProperties",
            &[
                InvocationArg::try_from(key)?,
                InvocationArg::try_from(value)?,
            ],
        )?;
    }

    // Set the property object for the mapper
    jvm.invoke(mapper, "setProperties", &[InvocationArg::from(props)])?;

    // Explicitly set the document object
    jvm.invoke(mapper, "setDocument", &[InvocationArg::from(document)])?;
    Ok(())
}
