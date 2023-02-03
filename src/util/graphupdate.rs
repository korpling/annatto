use std::path::{Path, PathBuf};

use crate::Result;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;

pub fn path_structure(
    u: &mut GraphUpdate,
    root_path: &Path,
    file_endings: &[&str],
    follow_links: bool,
) -> Result<Vec<(PathBuf, String)>> {
    todo!()
}

pub fn map_audio_source(u: &mut GraphUpdate, audio_path: &Path, corpus_path: &str) -> Result<()> {
    todo!("Implement map_audio_source")
}

pub fn add_order_relations(
    u: &mut GraphUpdate,
    node_ids: &[&str],
    order_name: Option<&str>,
) -> Result<()> {
    for i in 1..node_ids.len() {
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_ids[i - 1].to_string(),
            target_node: node_ids[i].to_string(),
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
