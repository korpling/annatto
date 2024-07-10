use crate::{
    error::{AnnattoError, Result},
    StepID,
};
use graphannis::{
    graph::EdgeContainer,
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};

use anyhow::Context;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{storage::union::UnionEdgeContainer, ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
    types::{Edge, NodeID},
};
use itertools::Itertools;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

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

#[allow(dead_code)]
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

/// Returns a sorted list of node names of all the corpus graph nodes without any outgoing `PartOf` edge.
pub(crate) fn get_root_corpus_node_names(graph: &AnnotationGraph) -> anyhow::Result<Vec<String>> {
    let mut roots: BTreeSet<String> = BTreeSet::new();
    let all_part_of_gs = graph
        .get_all_components(Some(AnnotationComponentType::PartOf), None)
        .into_iter()
        .filter_map(|c| graph.get_graphstorage(&c))
        .collect_vec();
    let all_part_of_edge_container = all_part_of_gs
        .iter()
        .map(|gs| gs.as_edgecontainer())
        .collect_vec();
    let part_of_gs = UnionEdgeContainer::new(all_part_of_edge_container);

    for candidate in graph.get_node_annos().exact_anno_search(
        Some(ANNIS_NS),
        NODE_TYPE,
        ValueSearch::Some("corpus"),
    ) {
        let candidate = candidate?;
        // Check if this target node is a root corpus node
        if !part_of_gs.has_outgoing_edges(candidate.node)? {
            let root_node_name = graph
                .get_node_annos()
                .get_value_for_item(&candidate.node, &NODE_NAME_KEY)?
                .context("Missing node name")?
                .to_string();
            roots.insert(root_node_name);
        }
    }

    Ok(roots.into_iter().collect_vec())
}

/// Returns a sorted list of node names of all the corpus graph nodes without any ingoing `PartOf` edge.
pub(crate) fn get_document_node_names(graph: &AnnotationGraph) -> anyhow::Result<Vec<String>> {
    let mut documents: BTreeSet<String> = BTreeSet::new();

    for candidate in
        graph
            .get_node_annos()
            .exact_anno_search(Some(ANNIS_NS), "doc", ValueSearch::Any)
    {
        let candidate = candidate?;
        let root_node_name = graph
            .get_node_annos()
            .get_value_for_item(&candidate.node, &NODE_NAME_KEY)?
            .context("Missing node name")?
            .to_string();
        documents.insert(root_node_name);
    }
    Ok(documents.into_iter().collect_vec())
}
