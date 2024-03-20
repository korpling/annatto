use crate::{
    error::{AnnattoError, Result},
    StepID,
};
use graphannis::{model::AnnotationComponent, AnnotationGraph};

use graphannis_core::types::{Edge, NodeID};
use std::path::{Path, PathBuf};

#[cfg(test)]
pub(crate) mod example_generator;
pub(crate) mod graphupdate;
pub(crate) mod token_helper;

/// Get all files with a given extension in a directory.
pub fn get_all_files(
    corpus_root_dir: &Path,
    file_extensions: &[&str],
) -> std::result::Result<Vec<PathBuf>, AnnattoError> {
    let mut paths = Vec::new();
    let flex_path = corpus_root_dir.join("**");
    for ext in file_extensions {
        let ext_path = flex_path.join(format!("*.{ext}"));
        for file_opt in glob::glob(&ext_path.to_string_lossy())? {
            paths.push(file_opt?)
        }
    }
    Ok(paths)
}

pub trait Traverse<N, E> {
    /// A node has been reached traversing the given component.
    fn node(
        &self,
        step_id: &StepID,
        graph: &AnnotationGraph,
        node: NodeID,
        component: &AnnotationComponent,
        buffer: &mut N,
    ) -> Result<()>;

    /// An edge is being processed while traversing the graph in the given component.
    fn edge(
        &self,
        step_id: &StepID,
        graph: &AnnotationGraph,
        edge: Edge,
        component: &AnnotationComponent,
        buffer: &mut E,
    ) -> Result<()>;

    fn traverse(
        &self,
        step_id: &StepID,
        graph: &AnnotationGraph,
        node_buffer: &mut N,
        edge_buffer: &mut E,
    ) -> Result<()>;
}
