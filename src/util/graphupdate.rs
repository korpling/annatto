use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::Result;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;

fn add_subcorpora(
    u: &mut GraphUpdate,
    file_path: &Path,
    parent_corpus: &str,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(file_path)? {
        let entry = entry?;
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
        } else if entry_type.is_dir() {
            true
        } else {
            false
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
                component_type: "PartOf".to_string(),
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

pub fn path_structure(
    u: &mut GraphUpdate,
    root_path: &Path,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let norm_path = root_path.canonicalize()?;
    let root_name = norm_path
        .file_name()
        .unwrap_or(OsStr::new("root-corpus"))
        .to_string_lossy();

    u.add_event(UpdateEvent::AddNode {
        node_name: root_name.to_string(),
        node_type: "corpus".to_string(),
    })?;

    let mut path_tuples = add_subcorpora(u, &norm_path, &root_name, file_endings)?;
    path_tuples.sort();
    Ok(path_tuples)
}

pub fn map_audio_source(u: &mut GraphUpdate, audio_path: &Path, corpus_path: &str) -> Result<()> {
    todo!("Implement map_audio_source")
}

pub fn add_order_relations<S: AsRef<str>>(
    u: &mut GraphUpdate,
    node_ids: &[S],
    order_name: Option<&str>,
) -> Result<()> {
    for i in 1..node_ids.len() {
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_ids[i - 1].as_ref().to_string(),
            target_node: node_ids[i].as_ref().to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: "Ordering".to_string(),
            component_name: order_name.unwrap_or_default().to_string(),
        })?;
    }
    Ok(())
}

pub fn map_token(
    u: &mut GraphUpdate,
    doc_path: &str,
    id: &str,
    text_name: Option<&str>,
    value: &str,
    start_time: Option<f64>,
    end_time: Option<f64>,
    add_annis_layer: bool,
) -> Result<String> {
    let tok_id = format!("{}#t{}", doc_path, id);
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
        target_node: doc_path.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: "PartOf".to_string(),
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
            format!("{}-{}", start_time, end_time)
        } else {
            format!("{}-", start_time)
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
    doc_path: &str,
    id: &str,
    ns: Option<&str>,
    name: Option<&str>,
    value: Option<&str>,
    targets: &[S],
) -> Result<String> {
    let span_id = format!("{}#sSpan{}", doc_path, id);
    u.add_event(UpdateEvent::AddNode {
        node_name: span_id.clone(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: span_id.to_string(),
        target_node: doc_path.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: "PartOf".to_string(),
        component_name: "".to_string(),
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
            component_type: "Coverage".to_string(),
            component_name: "".to_string(),
        })?;
    }

    Ok(span_id)
}
