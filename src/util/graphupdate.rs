use std::{
    borrow::Cow,
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::Result;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use normpath::PathExt;

fn add_subcorpora(
    u: &mut GraphUpdate,
    file_path: &Path,
    parent_corpus: &str,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let mut result = Vec::new();

    // Get the files and sort them according to their path, to get a predictable
    // order of adding the documents to the graph.
    let mut files_in_directory = Vec::new();
    for entry in std::fs::read_dir(file_path)? {
        let entry = entry?;
        files_in_directory.push(entry);
    }
    files_in_directory.sort_by_key(|dir_entry| dir_entry.path());

    for entry in files_in_directory {
        let entry_type = entry.file_type()?;
        let entry_path = entry.path();
        let subcorpus_name = entry_path
            .file_stem()
            .map(|f| f.to_os_string())
            .unwrap_or_else(|| entry.file_name())
            .to_string_lossy()
            .to_string();
        let node_name = format!("{}/{}", parent_corpus, subcorpus_name);
        let add_node = if entry_type.is_file() {
            if let Some(actual_ending) = entry.path().extension() {
                file_endings
                    .iter()
                    .any(|ext| *ext == actual_ending.to_string_lossy().as_ref())
            } else {
                false
            }
        } else {
            entry_type.is_dir()
        };
        if add_node {
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.clone(),
                node_type: "corpus".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: node_name.clone(),
                target_node: parent_corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;

            if entry_type.is_dir() {
                result.extend(add_subcorpora(u, &entry.path(), &node_name, file_endings)?);
            } else if entry_type.is_file() {
                // Also add the special "annis:doc" label to mark this as document
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "doc".to_string(),
                    anno_value: subcorpus_name.to_string(),
                })?;
                // Only add the corpus graph leafs to the result vector
                result.push((entry.path(), node_name));
            }
        }
    }
    Ok(result)
}

pub fn root_corpus_from_path(root_path: &Path) -> Result<String> {
    let norm_path = root_path.normalize()?;
    let root_name = norm_path
        .file_name()
        .unwrap_or_else(|| OsStr::new("root-corpus"))
        .to_string_lossy();

    Ok(root_name.to_string())
}

pub fn path_structure(
    u: &mut GraphUpdate,
    root_path: &Path,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let root_name = root_corpus_from_path(root_path)?;

    u.add_event(UpdateEvent::AddNode {
        node_name: root_name.clone(),
        node_type: "corpus".to_string(),
    })?;

    let mut path_tuples = add_subcorpora(u, root_path, &root_name, file_endings)?;
    path_tuples.sort();
    Ok(path_tuples)
}

pub fn map_audio_source(
    u: &mut GraphUpdate,
    audio_path: &Path,
    parent_corpus: &str,
    doc_path: &str,
) -> Result<String> {
    let node_name = format!(
        "{}/{}",
        parent_corpus,
        audio_path
            .file_name()
            .map_or_else(|| Cow::from("unknown_file"), |f| f.to_string_lossy())
    );
    u.add_event(UpdateEvent::AddNode {
        node_name: node_name.to_string(),
        node_type: "file".to_string(),
    })?;
    // TODO: make sure the file path is relative to the corpus directory. This
    // means we also have to implement copying the linked files to the same
    // location as the GraphML file.
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: node_name.to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "file".to_string(),
        anno_value: audio_path.to_string_lossy().to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: node_name.to_string(),
        target_node: doc_path.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;

    Ok(node_name)
}

pub fn add_order_relations<S: AsRef<str>>(
    u: &mut GraphUpdate,
    node_ids: &[S],
    order_name: Option<&str>,
) -> Result<()> {
    let ordering_layer = if order_name.is_none() {
        ANNIS_NS.to_owned()
    } else {
        DEFAULT_NS.to_owned()
    };
    for i in 1..node_ids.len() {
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_ids[i - 1].as_ref().to_string(),
            target_node: node_ids[i].as_ref().to_string(),
            layer: ordering_layer.clone(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: order_name.unwrap_or_default().to_string(),
        })?;
    }
    Ok(())
}

pub struct NodeInfo<'a> {
    node_id: &'a str,
    doc_path: &'a str,
    text_node_name: &'a str,
}

impl<'a> NodeInfo<'a> {
    pub fn new(node_id: &'a str, doc_path: &'a str, text_node_name: &'a str) -> NodeInfo<'a> {
        NodeInfo {
            node_id,
            doc_path,
            text_node_name,
        }
    }
}

pub fn map_token(
    u: &mut GraphUpdate,
    node: &NodeInfo,
    text_name: Option<&str>,
    value: &str,
    start_time: Option<f64>,
    end_time: Option<f64>,
    add_annis_layer: bool,
) -> Result<String> {
    let tok_id = format!("{}#t{}", node.doc_path, node.node_id);
    u.add_event(UpdateEvent::AddNode {
        node_name: tok_id.clone(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: tok_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok".to_string(),
        anno_value: value.to_string(),
    })?;
    if let Some(text_name) = text_name {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: "".to_string(),
            anno_name: text_name.to_string(),
            anno_value: value.to_string(),
        })?;
    };
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: tok_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok-whitespace-after".to_string(),
        anno_value: " ".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: tok_id.clone(),
        target_node: node.text_node_name.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    if let Some(start_time) = start_time {
        let time_code = if let Some(end_time) = end_time {
            if start_time >= end_time {
                return Err(crate::error::AnnattoError::EndTokenTimeLargerThanStart {
                    start: start_time,
                    end: end_time,
                });
            }
            format!("{start_time}-{end_time}")
        } else {
            format!("{start_time}-")
        };
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "time".to_string(),
            anno_value: time_code,
        })?;
    }
    if add_annis_layer {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
    }
    Ok(tok_id)
}

pub fn map_annotations<S: AsRef<str>>(
    u: &mut GraphUpdate,
    node: &NodeInfo,
    ns: Option<&str>,
    name: Option<&str>,
    value: Option<&str>,
    targets: &[S],
) -> Result<String> {
    let span_id = format!("{}#sSpan{}", node.doc_path, node.node_id);
    u.add_event(UpdateEvent::AddNode {
        node_name: span_id.clone(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: span_id.to_string(),
        target_node: node.text_node_name.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: span_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "layer".to_string(),
        anno_value: "default_ns".to_string(),
    })?;
    if let Some(name) = name {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: span_id.clone(),
            anno_ns: ns.unwrap_or_default().to_string(),
            anno_name: name.to_string(),
            anno_value: value.unwrap_or_default().to_string(),
        })?;
    }
    for target in targets {
        u.add_event(UpdateEvent::AddEdge {
            source_node: span_id.clone(),
            target_node: target.as_ref().to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
    }

    Ok(span_id)
}
