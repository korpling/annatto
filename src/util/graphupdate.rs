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
    doc_path: &Path,
    id: &str,
    text_name: &str,
    value: &str,
    start_time: Option<f64>,
    end_time: Option<f64>,
    add_annis_layer: bool,
) -> Result<()> {
    todo!("Implement map_token")
}
