use std::path::{Path, PathBuf};

use crate::Result;
use graphannis::update::GraphUpdate;

pub fn path_structure(
    u: &mut GraphUpdate,
    root_path: &Path,
    file_endings: &[&str],
    follow_links: bool,
) -> Result<Vec<(PathBuf, String)>> {
    todo!()
}

pub fn map_audio_source(u: &mut GraphUpdate, audio_path: &Path, corpus_path: &str) -> Result<()> {
    todo!()
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
    todo!()
}
