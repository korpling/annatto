use crate::{
    error::{AnnattoError, Result},
    StepID,
};
use graphannis::{
    graph::{EdgeContainer, GraphStorage},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};

use anyhow::Context;
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE, NODE_TYPE_KEY},
    types::{Edge, NodeID},
};
use itertools::Itertools;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
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

/// Provides utility functions for corpus and document nodes which form the
/// corpus graph.
///
/// This struct also implements [`EdgeContainer`] by providing an union of all
/// [`AnnotationComponentType::PartOf`] graph components.
pub(crate) struct CorpusGraphHelper<'a> {
    graph: &'a AnnotationGraph,
    all_partof_gs: Vec<Arc<dyn GraphStorage>>,
}

impl<'a> CorpusGraphHelper<'a> {
    pub(crate) fn new(graph: &'a AnnotationGraph) -> Self {
        let all_partof_gs: Vec<_> = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .into_iter()
            .filter_map(|c| graph.get_graphstorage(&c))
            .collect();
        CorpusGraphHelper {
            graph,
            all_partof_gs,
        }
    }

    /// Returns a list of node names of all the corpus graph nodes without any
    /// outgoing `PartOf` edge.
    pub(crate) fn get_root_corpus_node_names(&self) -> anyhow::Result<Vec<String>> {
        let mut roots: BTreeSet<String> = BTreeSet::new();

        let node_annos = self.graph.get_node_annos();

        for candidate in
            node_annos.exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("corpus"))
        {
            let candidate = candidate?;
            // Check if this target node is a root corpus node
            if !self.has_outgoing_edges(candidate.node)? {
                let root_node_name = node_annos
                    .get_value_for_item(&candidate.node, &NODE_NAME_KEY)?
                    .context("Missing node name")?
                    .to_string();
                roots.insert(root_node_name);
            }
        }

        Ok(roots.into_iter().collect_vec())
    }

    /// Returns a list of node names nodes of the corpus graph that are documents.
    ///
    /// Documents have no ingoing edges from other nodes of the type "corpus".
    pub(crate) fn get_document_node_names(&self) -> anyhow::Result<Vec<String>> {
        let mut documents: BTreeSet<String> = BTreeSet::new();

        let node_annos = self.graph.get_node_annos();

        for candidate in
            node_annos.exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("corpus"))
        {
            let candidate = candidate?;

            if self.is_document(candidate.node)? {
                let candidate_node_name = node_annos
                    .get_value_for_item(&candidate.node, &NODE_NAME_KEY)?
                    .context("Missing node name")?
                    .to_string();

                documents.insert(candidate_node_name);
            }
        }
        Ok(documents.into_iter().collect_vec())
    }

    /// Checks if the given node is a document in the strcutural sense.
    ///
    /// Having a `annis:doc` label is not necessary, but the node type must be
    /// `corpus` and there must be no incoming [`AnnotationComponentType::PartOf`] edges
    /// from other corpus nodes.
    pub(crate) fn is_document(&self, node: NodeID) -> anyhow::Result<bool> {
        let node_annos = self.graph.get_node_annos();

        let node_type = node_annos
            .get_value_for_item(&node, &NODE_TYPE_KEY)?
            .context("Missing node type")?;
        if node_type != "corpus" {
            return Ok(false);
        }
        for ingoing in self.get_ingoing_edges(node) {
            let ingoing = ingoing?;
            if let Some(ingoing_node_type) =
                node_annos.get_value_for_item(&ingoing, &NODE_TYPE_KEY)?
            {
                if ingoing_node_type == "corpus" {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Returns true if there is a path from the `child` node to the `ancestor` node in any of the [`AnnotationComponentType::PartOf`] components.
    pub(crate) fn is_part_of(&self, child: NodeID, ancestor: NodeID) -> anyhow::Result<bool> {
        if self.all_partof_gs.len() == 1 {
            let connected = self.all_partof_gs[0].is_connected(
                child,
                ancestor,
                1,
                std::ops::Bound::Unbounded,
            )?;
            return Ok(connected);
        } else {
            for step in CycleSafeDFS::new(self, child, 1, usize::MAX) {
                let step = step?;
                if step.node == ancestor {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Find all nodes that are part of the given ancestor node.
    pub(crate) fn all_nodes_part_of(
        &'a self,
        ancestor: NodeID,
    ) -> Box<dyn Iterator<Item = std::result::Result<u64, GraphAnnisCoreError>> + 'a> {
        if self.all_partof_gs.len() == 1 {
            let it = self.all_partof_gs[0].find_connected_inverse(
                ancestor,
                1,
                std::ops::Bound::Unbounded,
            );
            it
        } else {
            let it =
                CycleSafeDFS::new_inverse(self, ancestor, 1, usize::MAX).map_ok(|step| step.node);
            Box::new(it)
        }
    }
}

impl<'c> EdgeContainer for CorpusGraphHelper<'c> {
    fn get_outgoing_edges<'a>(
        &'a self,
        node: NodeID,
    ) -> Box<dyn Iterator<Item = graphannis_core::errors::Result<NodeID>> + 'a> {
        let it = self
            .all_partof_gs
            .iter()
            .flat_map(move |gs| gs.get_outgoing_edges(node));
        Box::new(it)
    }

    fn get_ingoing_edges<'a>(
        &'a self,
        node: NodeID,
    ) -> Box<dyn Iterator<Item = graphannis_core::errors::Result<NodeID>> + 'a> {
        let it = self
            .all_partof_gs
            .iter()
            .flat_map(move |gs| gs.get_ingoing_edges(node));
        Box::new(it)
    }

    fn source_nodes<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = graphannis_core::errors::Result<NodeID>> + 'a> {
        let it = self
            .all_partof_gs
            .iter()
            .flat_map(move |gs| gs.source_nodes());
        Box::new(it)
    }
}
