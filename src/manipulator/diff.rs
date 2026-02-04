use std::{collections::BTreeMap, sync::Arc, usize};

use anyhow::{anyhow, bail};
use facet::Facet;
use graphannis::{
    AnnotationGraph, aql,
    graph::{AnnoKey, EdgeContainer, GraphStorage, Match, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::{CycleSafeDFS, DFSStep},
    graph::{ANNIS_NS, NODE_NAME_KEY, storage::union::UnionEdgeContainer},
};
use itertools::Itertools;
use likewise::{Algorithm, DiffOp, capture_diff_slices};
use serde::{Deserialize, Serialize};

use crate::{manipulator::Manipulator, progress::ProgressReporter, util::update_graph_silent};

/// Compare to sub graphs, derive a patch from one towards the other,
/// and apply it.
#[derive(Clone, Deserialize, Facet, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiffSubgraphs {
    /// Provide an annotation key that distinguishes relevant sub graphs to match
    /// differences between. Default is `annis::doc`, which means that diffs are
    /// annotated by comparing documents with the same name in different subgraphs.
    #[serde(with = "crate::estarde::anno_key", default = "default_by_key")]
    by: AnnoKey,
    /// The node name of the common parent of all source parent nodes matching the `by` key.
    /// If you are importing your source data for comparison from a directory "path/to/a",
    /// the value to be set here is "a". If you are importing source and target of the diff
    /// comparison in one import and the data is in different subfolders of the import directory,
    /// you have to qualify the path a little further, e. g. "data/a", if you are importing
    /// from directory "data".
    source_parent: String,
    /// Provide the source component along which the source sequence of comparison
    /// is to be determined (usually an ordering).
    #[serde(with = "crate::estarde::annotation_component")]
    source_component: AnnotationComponent,
    /// This annotation key determines the values in the source sequence.
    #[serde(with = "crate::estarde::anno_key")]
    source_key: AnnoKey,
    /// The node name of the common parent of all target parent nodes matching the `by` key.
    /// If you are importing your target data for comparison from a directory "path/to/b",
    /// the value to be set here is "b". For more details see above.
    target_parent: String,
    /// Provide the target component along which the target sequence of comparison
    /// is to be determined (usually an ordering).
    #[serde(with = "crate::estarde::annotation_component")]
    target_component: AnnotationComponent,
    /// This annotation key determines the values in the target sequence.
    #[serde(with = "crate::estarde::anno_key")]
    target_key: AnnoKey,
    /// Define the diff algorithm. Options are `lcs`, `myers`, and `patience` (default).
    #[serde(default)]
    algorithm: DiffAlgorithm,
    /// Directly merge the two subgraphs instead of creating diff annotations.
    /// Example:
    ///
    /// ```toml
    /// [graph_op.config]
    /// merge = true
    /// ```
    ///
    /// Default is `false`.
    #[serde(default)]
    merge: bool,
}

fn default_by_key() -> AnnoKey {
    AnnoKey {
        ns: ANNIS_NS.into(),
        name: "doc".into(),
    }
}

#[derive(Clone, Default, Deserialize, Facet, PartialEq, Serialize)]
#[repr(u8)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
enum DiffAlgorithm {
    Lcs,
    Myers,
    #[default]
    Patience,
}

impl From<DiffAlgorithm> for Algorithm {
    fn from(value: DiffAlgorithm) -> Self {
        match value {
            DiffAlgorithm::Lcs => Algorithm::Lcs,
            DiffAlgorithm::Myers => Algorithm::Myers,
            DiffAlgorithm::Patience => Algorithm::Patience,
        }
    }
}

impl Manipulator for DiffSubgraphs {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let mut graph_helper =
            GraphDiffHelper::new(graph, &self.source_component, &self.target_component)?;
        let pairs = self.pair(&graph_helper, &progress)?;
        let progress = ProgressReporter::new(tx.clone(), step_id, pairs.len())?;
        for pair in pairs {
            if self.merge {
                pair.merge_diff(
                    &mut graph_helper,
                    &self.source_key,
                    &self.target_key,
                    self.algorithm.clone(),
                )?;
            } else {
                pair.annotate_diff(
                    &graph_helper,
                    &self.source_key,
                    &self.target_key,
                    self.algorithm.clone(),
                    &mut update,
                )?;
            }
            progress.worked(1)?;
        }
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

struct GraphDiffHelper<'a> {
    graph: &'a mut AnnotationGraph,
    component_a: &'a AnnotationComponent,
    component_b: &'a AnnotationComponent,
}

impl<'a> GraphDiffHelper<'a> {
    fn new(
        graph: &'a mut AnnotationGraph,
        component_a: &'a AnnotationComponent,
        component_b: &'a AnnotationComponent,
    ) -> Result<Self, anyhow::Error> {
        Ok(GraphDiffHelper {
            graph,
            component_a,
            component_b,
        })
    }

    fn node_name(&self, node: NodeID) -> Result<String, anyhow::Error> {
        Ok(self
            .graph
            .get_node_annos()
            .get_value_for_item(&node, &NODE_NAME_KEY)?
            .ok_or(anyhow!("Node has no name."))?
            .to_string())
    }

    fn get_parent(&self, node: NodeID) -> Result<String, anyhow::Error> {
        let part_of_storages = self
            .graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .iter()
            .map(|c| self.graph.get_graphstorage(c))
            .flatten()
            .collect_vec();
        let container = UnionEdgeContainer::new(
            part_of_storages
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect(),
        );
        let parent = container
            .get_outgoing_edges(node)
            .next()
            .ok_or(anyhow!("Node has no parent"))??;
        self.node_name(parent)
    }

    fn sequence(
        &self,
        component_storage: Arc<dyn GraphStorage>,
        parent: NodeID,
    ) -> Result<Vec<NodeID>, anyhow::Error> {
        let part_of_storages = self
            .graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .iter()
            .map(|c| self.graph.get_graphstorage(c))
            .flatten()
            .collect_vec();
        let part_of_container = UnionEdgeContainer::new(
            part_of_storages
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect(),
        );
        let dfs = CycleSafeDFS::new_inverse(&part_of_container, parent, 0, usize::MAX).flatten();
        let mut random_seq_node = None;
        for s in dfs {
            let n = s.node;
            if component_storage.has_ingoing_edges(n)? || component_storage.has_outgoing_edges(n)? {
                random_seq_node = Some(n);
                break;
            }
        }
        let ordered_node = random_seq_node.ok_or(anyhow!(
            "No ordered source node found via part of components."
        ))?;
        let mut ordered_nodes = component_storage
            .find_connected_inverse(ordered_node, 0, std::ops::Bound::Unbounded)
            .flatten()
            .collect_vec();
        ordered_nodes.reverse();
        ordered_nodes.extend(
            component_storage
                .find_connected(ordered_node, 1, std::ops::Bound::Unbounded)
                .flatten(),
        );
        Ok(ordered_nodes)
    }

    fn sequences(
        &self,
        source_parent: NodeID,
        target_parent: NodeID,
    ) -> Result<(Vec<NodeID>, Vec<NodeID>), anyhow::Error> {
        Ok((
            self.sequence(
                self.graph
                    .get_graphstorage(self.component_a)
                    .ok_or(anyhow!(
                        "Component storage could not be obtained: {:?}",
                        self.component_a
                    ))?,
                source_parent,
            )?,
            self.sequence(
                self.graph
                    .get_graphstorage(self.component_b)
                    .ok_or(anyhow!(
                        "Component storage could not be obtained: {:?}",
                        self.component_b
                    ))?,
                target_parent,
            )?,
        ))
    }

    fn apply_update(&mut self, update: &mut GraphUpdate) -> Result<(), anyhow::Error> {
        update_graph_silent(self.graph, update)?;
        Ok(())
    }

    fn calculate_statistics(&mut self) -> Result<(), anyhow::Error> {
        self.graph.calculate_all_statistics()?;
        Ok(())
    }

    fn graph(&'a self) -> &'a AnnotationGraph {
        self.graph
    }
}

impl<'a> DiffSubgraphs {
    fn pair(
        &self,
        graph_helper: &GraphDiffHelper<'a>,
        progress: &ProgressReporter,
    ) -> Result<Vec<SequencePair>, anyhow::Error> {
        let graph = graph_helper.graph();
        let node_annos = graph.get_node_annos();
        let by_values = node_annos.get_all_values(&self.by, false)?;
        let mut sequence_pairs = Vec::with_capacity(by_values.len());
        for value in by_values {
            let matching = node_annos
                .exact_anno_search(Some(&self.by.ns), &self.by.name, ValueSearch::Some(&value))
                .flatten()
                .collect_vec();
            if matching.len() != 2 {
                progress.warn(format!(
                    "Cannot create diff for {} nodes with by-value `{value}`. Must be exactly 2.",
                    matching.len()
                ))?;
                continue;
            }
            let (source_index, target_index) = {
                let first_name = graph_helper.get_parent(matching[0].node)?;
                let second_name = graph_helper.get_parent(matching[1].node)?;
                if first_name == self.source_parent && second_name == self.target_parent {
                    (0, 1)
                } else if first_name == self.target_parent && second_name == self.source_parent {
                    (1, 0)
                } else {
                    bail!("Could not determine source and target subgraph from match for by key.");
                }
            };
            let source_parent = matching[source_index].node;
            let target_parent = matching[target_index].node;
            let (ordered_source_nodes, ordered_target_nodes) =
                graph_helper.sequences(source_parent, target_parent)?;
            let seq_pair = SequencePair {
                source_stem: graph_helper.node_name(source_parent)?,
                target_stem: graph_helper.node_name(target_parent)?,
                source_nodes: ordered_source_nodes,
                target_nodes: ordered_target_nodes,
            };
            sequence_pairs.push(seq_pair);
        }
        Ok(sequence_pairs)
    }
}

struct SequencePair {
    source_stem: String,
    target_stem: String,
    source_nodes: Vec<NodeID>,
    target_nodes: Vec<NodeID>,
}

impl SequencePair {
    fn compute_diff(
        &self,
        helper: &GraphDiffHelper,
        source_key: &AnnoKey,
        target_key: &AnnoKey,
        algorithm: DiffAlgorithm,
    ) -> Result<Vec<DiffOp>, anyhow::Error> {
        let mut vocabulary = BTreeMap::default();
        let (a, b) = {
            let (mut a, mut b) = (
                Vec::with_capacity(self.source_nodes.len()),
                Vec::with_capacity(self.target_nodes.len()),
            );
            for ((node_id_src, value_target), key) in [&self.source_nodes, &self.target_nodes]
                .into_iter()
                .zip([&mut a, &mut b])
                .zip([source_key, target_key])
            {
                for n in node_id_src {
                    if let Some(v) = helper.graph().get_node_annos().get_value_for_item(n, key)? {
                        if let Some(index) = vocabulary.get(&v) {
                            value_target.push(*index);
                        } else {
                            value_target.push(vocabulary.len());
                            vocabulary.insert(v, vocabulary.len());
                        }
                    }
                }
            }
            (a, b)
        };
        Ok(capture_diff_slices(algorithm.into(), &a, &b))
    }

    fn annotate_diff(
        self,
        helper: &GraphDiffHelper,
        source_key: &AnnoKey,
        target_key: &AnnoKey,
        algorithm: DiffAlgorithm,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        for d in self.compute_diff(helper, source_key, target_key, algorithm)? {
            match d {
                DiffOp::Equal {
                    old_index,
                    new_index,
                    len,
                } => {
                    for (src_node, tgt_node) in self.source_nodes[old_index..old_index + len]
                        .iter()
                        .zip_eq(&self.target_nodes[new_index..new_index + len])
                    {
                        let old_node_name = helper.node_name(*src_node)?;
                        let new_node_name = helper.node_name(*tgt_node)?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: old_node_name.to_string(),
                            target_node: new_node_name.to_string(),
                            layer: "".to_string(),
                            component_type: AnnotationComponentType::Pointing.to_string(),
                            component_name: "diff".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdgeLabel {
                            source_node: old_node_name.to_string(),
                            target_node: new_node_name.to_string(),
                            layer: "".to_string(),
                            component_type: AnnotationComponentType::Pointing.to_string(),
                            component_name: "diff".to_string(),
                            anno_ns: "diff".to_string(),
                            anno_name: "op".to_string(),
                            anno_value: "=".to_string(),
                        })?;
                    }
                }
                DiffOp::Delete {
                    old_index, old_len, ..
                } => {
                    for node_id in &self.source_nodes[old_index..old_index + old_len] {
                        let old_node_name = helper.node_name(*node_id)?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: old_node_name.to_string(),
                            anno_ns: "diff".to_string(),
                            anno_name: "op".to_string(),
                            anno_value: "-".to_string(),
                        })?;
                    }
                }
                DiffOp::Insert {
                    new_index, new_len, ..
                } => {
                    for node_id in &self.target_nodes[new_index..new_index + new_len] {
                        let new_node_name = helper.node_name(*node_id)?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: new_node_name.to_string(),
                            anno_ns: "diff".to_string(),
                            anno_name: "op".to_string(),
                            anno_value: "+".to_string(),
                        })?;
                    }
                }
                DiffOp::Replace {
                    old_index,
                    old_len,
                    new_index,
                    new_len,
                } => {
                    // create span over old segment
                    let old_span_name = format!("{}#sub_{old_index}_{old_len}", self.source_stem);
                    update.add_event(UpdateEvent::AddNode {
                        node_name: old_span_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: old_span_name.to_string(),
                        anno_ns: "diff".to_string(),
                        anno_name: "op".to_string(),
                        anno_value: "sub".to_string(),
                    })?;
                    for src_node in &self.source_nodes[old_index..old_index + old_len] {
                        let src_node_name = helper.node_name(*src_node)?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: old_span_name.to_string(),
                            target_node: src_node_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    let new_span_name = format!("{}#sub_{new_index}_{new_len}", self.target_stem);
                    update.add_event(UpdateEvent::AddNode {
                        node_name: new_span_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: new_span_name.to_string(),
                        anno_ns: "diff".to_string(),
                        anno_name: "op".to_string(),
                        anno_value: "sub".to_string(),
                    })?;
                    for tgt_node in &self.target_nodes[new_index..new_index + new_len] {
                        let tgt_node_name = helper.node_name(*tgt_node)?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: new_span_name.to_string(),
                            target_node: tgt_node_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: old_span_name.to_string(),
                        target_node: new_span_name.to_string(),
                        layer: "".to_string(),
                        component_type: AnnotationComponentType::Pointing.to_string(),
                        component_name: "diff".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddEdgeLabel {
                        source_node: old_span_name.to_string(),
                        target_node: new_span_name.to_string(),
                        layer: "".to_string(),
                        component_type: AnnotationComponentType::Pointing.to_string(),
                        component_name: "diff".to_string(),
                        anno_ns: "diff".to_string(),
                        anno_name: "op".to_string(),
                        anno_value: "sub".to_string(),
                    })?;
                }
            };
        }
        Ok(())
    }

    fn merge_diff(
        mut self,
        helper: &mut GraphDiffHelper,
        source_key: &AnnoKey,
        target_key: &AnnoKey,
        algorithm: DiffAlgorithm,
    ) -> Result<(), anyhow::Error> {
        let diff = self.compute_diff(helper, source_key, target_key, algorithm)?;
        // get to work
        for d in diff {
            let mut update = GraphUpdate::default();
            match d {
                DiffOp::Equal { new_index, len, .. } => {
                    let coverage_storages = helper
                        .graph
                        .get_all_components(Some(AnnotationComponentType::Coverage), None)
                        .iter()
                        .flat_map(|c| helper.graph.get_graphstorage(c))
                        .collect_vec();
                    let coverage_container = UnionEdgeContainer::new(
                        coverage_storages
                            .iter()
                            .map(|gs| gs.as_edgecontainer())
                            .collect_vec(),
                    );
                    for node in &self.target_nodes[new_index..new_index + len] {
                        let node_name = helper.node_name(*node)?;
                        update.add_event(UpdateEvent::DeleteNode { node_name })?;
                        for reachable_node in
                            CycleSafeDFS::new(&coverage_container, *node, 1, usize::MAX)
                                .flatten()
                                .map(|DFSStep { node, .. }| node)
                        {
                            let reachable_node_name = helper.node_name(reachable_node)?;
                            update.add_event(UpdateEvent::DeleteNode {
                                node_name: reachable_node_name,
                            })?;
                        }
                        // now back up from terminals
                    }
                }
                DiffOp::Delete {
                    old_index, old_len, ..
                } => {
                    // set up containers (we need to do this here every time to not interfere with the update process,
                    // as Arc<GraphStorage> blocks mutable references for a ref-count > 1
                    let coverage_storages = helper
                        .graph
                        .get_all_components(Some(AnnotationComponentType::Coverage), None)
                        .iter()
                        .flat_map(|c| helper.graph.get_graphstorage(c))
                        .collect_vec();
                    let coverage_container = UnionEdgeContainer::new(
                        coverage_storages
                            .iter()
                            .map(|gs| gs.as_edgecontainer())
                            .collect_vec(),
                    );
                    for node in &self.source_nodes[old_index..old_index + old_len] {
                        // flag directly compared nodes for deletion in old graph
                        let node_name = helper.node_name(*node)?;
                        update.add_event(UpdateEvent::DeleteNode {
                            node_name: node_name.to_string(),
                        })?;
                        // delete covered coverage terminals
                        for reachable in
                            CycleSafeDFS::new(&coverage_container, *node, 1, usize::MAX).flatten()
                        {
                            if !coverage_container.has_outgoing_edges(reachable.node)? {
                                // flag coverage terminals for deletion, non-terminals might cover remaining nodes and need to be checked later
                                let delete_name = helper.node_name(reachable.node)?;
                                update.add_event(UpdateEvent::DeleteNode {
                                    node_name: delete_name.to_string(),
                                })?;
                            }
                        }
                    }
                    // Fix ordering
                    let first_node = *self.source_nodes.first().ok_or(anyhow!(
                        "Could not retrieve start of deleted sequence in original subgraph."
                    ))?;
                    let last_node = *self.source_nodes.last().ok_or(anyhow!(
                        "Could not retrieve end of deleted sequence in original subgraph."
                    ))?;
                    for ordering_component in helper
                        .graph
                        .get_all_components(Some(AnnotationComponentType::Ordering), None)
                    {
                        let gs =
                            helper
                                .graph
                                .get_graphstorage(&ordering_component)
                                .ok_or(anyhow!(
                                    "Could not load graph storage of component {:?}",
                                    ordering_component
                                ))?;
                        // strict assumption: Ordering components are not branching out
                        if let Some(from_node) = gs.get_ingoing_edges(first_node).flatten().next()
                            && let Some(to_node) = gs.get_outgoing_edges(last_node).flatten().next()
                        {
                            // bridge the gap
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: helper.node_name(from_node)?.to_string(),
                                target_node: helper.node_name(to_node)?.to_string(),
                                layer: ordering_component.layer.to_string(),
                                component_type: ordering_component.get_type().to_string(),
                                component_name: ordering_component.name.to_string(),
                            })?;
                        }
                    }
                }
                DiffOp::Insert {
                    old_index,
                    new_index,
                    new_len,
                } => {
                    self.replace_or_insert(
                        helper,
                        old_index,
                        None,
                        new_index,
                        new_len,
                        &mut update,
                    )?;
                }
                DiffOp::Replace {
                    old_index,
                    old_len,
                    new_index,
                    new_len,
                } => {
                    self.replace_or_insert(
                        helper,
                        old_index,
                        Some(old_len),
                        new_index,
                        new_len,
                        &mut update,
                    )?;
                }
            }
            // update after each diff op is required to allow follow-up ops to build on previous modifications, e. g. deletion after insertion
            helper.apply_update(&mut update)?;
        }
        {
            // final clean-up  (this seems to be a bad idea)
            let mut update = GraphUpdate::default();
            let query_for_deletion = aql::parse(
                &format!(
                    r#"node @* node_name="{}" & #1 !@* node_name="{}"?"#,
                    self.target_stem, self.source_stem
                ),
                false,
            )?;
            for m in aql::execute_query_on_graph(helper.graph, &query_for_deletion, true, None)?
                .flatten()
            {
                if let Some(Match { node, .. }) = m.get(0) {
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: helper.node_name(*node)?,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn replace_or_insert(
        &mut self,
        helper: &mut GraphDiffHelper,
        old_index: usize,
        old_len: Option<usize>,
        new_index: usize,
        new_len: usize,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        if helper.graph.global_statistics.is_none() {
            helper.calculate_statistics()?; // this is unfortunate, but well
        }
        // setup
        let coverage_storages = helper
            .graph
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .iter()
            .flat_map(|c| helper.graph.get_graphstorage(c))
            .collect_vec();
        let coverage_container = UnionEdgeContainer::new(
            coverage_storages
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect_vec(),
        );
        let default_ordering = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.to_string(),
            "".to_string(),
        );
        let default_ordering_gs = helper
            .graph
            .get_graphstorage(&default_ordering)
            .ok_or(anyhow!("Could not obtain storage of default ordering."))?;
        let insert_at_node = if old_index == 0 {
            // insertion before existing nodes or replacement at first index;
            // no new edge from predecessor needs to be built
            None
        } else {
            Some(
                *self
                    .source_nodes
                    .get(old_index - 1)
                    .ok_or(anyhow!("Could not obtain node for start of insertion."))?,
            )
        };
        let right_end_node = if let Some(len_value) = old_len {
            Some(
                *self
                    .source_nodes
                    .get(old_index + len_value - 1)
                    .ok_or(anyhow!(
                        "Could not determine node at right end of the sequence to be replaced"
                    ))?,
            )
        } else {
            insert_at_node
        };
        let insert_at_node_name = insert_at_node.and_then(|nid| {
            // warning, this buries the unlikely case of the anno storage not providing a value due to graphannis errors
            if let Ok(s) = helper.node_name(nid) {
                Some(s)
            } else {
                None
            }
        });
        let right_end_name = right_end_node.and_then(|nid| {
            // warning, this buries the unlikely case of the anno storage not providing a value due to graphannis errors
            if let Ok(s) = helper.node_name(nid) {
                Some(s)
            } else {
                None
            }
        }); // FIXME can this be deleted when old_len is Some?
        let start_of_insertion_sequence = *self.target_nodes.get(new_index).ok_or(anyhow!(
            "Could not obtain start node of sequence to be inserted."
        ))?;
        let start_of_insertion_sequence_name = helper.node_name(start_of_insertion_sequence)?;
        let end_of_insertion_sequence =
            *self
                .target_nodes
                .get(new_index + new_len - 1)
                .ok_or(anyhow!(
                    "Could not obtain end node of sequence to be inserted."
                ))?;
        let end_of_insertion_sequence_name = helper.node_name(end_of_insertion_sequence)?;
        for oc in helper
            .graph
            .get_all_components(Some(AnnotationComponentType::Ordering), None)
        {
            let gs = helper
                .graph
                .get_graphstorage(&oc)
                .ok_or(anyhow!("Could not obtain storage of {oc}"))?;
            if let Some(insert_node_at_node_id) = &insert_at_node {
                if !gs.has_ingoing_edges(*insert_node_at_node_id)?
                    && !gs.has_outgoing_edges(*insert_node_at_node_id)?
                {
                    // we are looking at the wrong ordering
                    continue;
                }
                if let Some(len_value) = old_len
                    && let Some(old_sequence_start) = gs
                        .get_outgoing_edges(*insert_node_at_node_id)
                        .flatten()
                        .next()
                {
                    for reachable_ordered_node in gs
                        .find_connected(old_sequence_start, 0, std::ops::Bound::Excluded(len_value))
                        .flatten()
                    {
                        let delete_node_name = helper.node_name(reachable_ordered_node)?;
                        update.add_event(UpdateEvent::DeleteNode {
                            node_name: delete_node_name,
                        })?;
                    }
                }
            }
            let m3 = if let Some(right_end_node_id) = &right_end_node
                && let Some(right_end_node_name) = &right_end_name
                && let Some(old_right_successor) =
                    gs.get_outgoing_edges(*right_end_node_id).flatten().next()
            {
                let old_right_successor_name = helper.node_name(old_right_successor)?; // this is the same as `old_successor_name` for the case of insertion (old_len.is_none() == true)
                if old_len.is_some() {
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: right_end_node_name.to_string(),
                    })?;
                } else {
                    update.add_event(UpdateEvent::DeleteEdge {
                        source_node: right_end_node_name.to_string(),
                        target_node: old_right_successor_name.to_string(),
                        layer: oc.layer.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: oc.name.to_string(),
                    })?;
                }
                update.add_event(UpdateEvent::AddEdge {
                    source_node: end_of_insertion_sequence_name.to_string(),
                    target_node: old_right_successor_name.to_string(),
                    layer: oc.layer.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: oc.name.to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: end_of_insertion_sequence_name.to_string(),
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                //
                let query_for_successor_tok = aql::parse(
                    &format!(r#"annis:node_name="{}" _l_ tok"#, &old_right_successor_name),
                    false,
                )?;
                aql::execute_query_on_graph(helper.graph, &query_for_successor_tok, true, None)?
                    .flatten()
                    .next()
            } else {
                None
            };
            if let Some(node_name) = &insert_at_node_name {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: start_of_insertion_sequence_name.to_string(),
                    layer: oc.layer.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: oc.name.to_string(),
                })?;
            }
            update.add_event(UpdateEvent::AddEdge {
                source_node: start_of_insertion_sequence_name.to_string(),
                target_node: self.source_stem.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            // now the toks have to be taken care of as well
            let query_for_insertion_start_tok = aql::parse(
                &format!(
                    r#"annis:node_name="{}" _l_ tok"#,
                    &start_of_insertion_sequence_name
                ),
                false,
            )?;
            let query_for_insertion_end_tok = aql::parse(
                &format!(
                    r#"annis:node_name="{}" _r_ tok"#,
                    &end_of_insertion_sequence_name
                ),
                false,
            )?;
            let m0 = if let Some(node_name) = &insert_at_node_name {
                let query_for_insertion_at_tok = aql::parse(
                    &format!(r#"annis:node_name="{}" _r_ tok"#, node_name),
                    false,
                )?;
                aql::execute_query_on_graph(helper.graph, &query_for_insertion_at_tok, true, None)?
                    .flatten()
                    .next()
            } else {
                None
            };
            let m1 = aql::execute_query_on_graph(
                helper.graph,
                &query_for_insertion_start_tok,
                true,
                None,
            )?
            .flatten()
            .next();
            let m2 = aql::execute_query_on_graph(
                helper.graph,
                &query_for_insertion_end_tok,
                true,
                None,
            )?
            .flatten()
            .next();
            let remaining_ordering_components = helper
                .graph
                .get_all_components(Some(AnnotationComponentType::Ordering), None)
                .into_iter()
                .filter(|c| {
                    (&c.layer != ANNIS_NS || &c.name != "")
                        && (c.layer != oc.layer || c.name != oc.name)
                })
                .collect_vec();
            let mut new_tok_sequence_start = None;
            if let Some(res_m0) = m0
                && let Some(res_m1) = m1
            {
                let link_source = res_m0
                    .get(1)
                    .ok_or(anyhow!("Result cannot be parsed."))?
                    .node;
                if let Some(old_successor_tok) = default_ordering_gs
                    .get_outgoing_edges(link_source)
                    .flatten()
                    .next()
                {
                    update.add_event(UpdateEvent::DeleteEdge {
                        source_node: helper.node_name(link_source)?,
                        target_node: helper.node_name(old_successor_tok)?,
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                let link_target = res_m1
                    .get(1)
                    .ok_or(anyhow!("Result cannot be parsed."))?
                    .node;
                new_tok_sequence_start = Some(link_target);
                let mut dfs = CycleSafeDFS::new(
                    default_ordering_gs.as_edgecontainer(),
                    link_source,
                    1,
                    usize::MAX,
                );
                let old_right_successor_tok =
                    m3.as_ref().map(|m| m.get(1).map(|e| e.node)).flatten();
                while let Some(Ok(DFSStep {
                    node: follow_up_node,
                    ..
                })) = dfs.next()
                    && (old_right_successor_tok.is_none()
                        || old_right_successor_tok
                            .map(|u| u != follow_up_node)
                            .unwrap_or_default())
                {
                    // delete all tok nodes in the old graph that are not used anymore
                    let delete_tok_name = helper.node_name(follow_up_node)?;
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: delete_tok_name,
                    })?;
                }
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(link_source)?,
                    target_node: helper.node_name(link_target)?,
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(link_target)?,
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                let reachable_from_left =
                    CycleSafeDFS::new_inverse(&coverage_container, link_source, 1, usize::MAX)
                        .into_iter()
                        .flatten()
                        .map(|s| {
                            let mut gap_starts =
                                Vec::with_capacity(remaining_ordering_components.len());
                            for rc in &remaining_ordering_components {
                                if let Some(gs) = helper.graph.get_graphstorage(rc)
                                    && (gs.has_outgoing_edges(s.node).unwrap_or_default()
                                        || gs.has_ingoing_edges(s.node).unwrap_or_default())
                                {
                                    gap_starts.push((rc, s.node));
                                }
                            }
                            gap_starts
                        })
                        .flatten()
                        .collect_vec();
                let reachable_from_right =
                    CycleSafeDFS::new_inverse(&coverage_container, link_target, 1, usize::MAX)
                        .into_iter()
                        .flatten()
                        .map(|s| {
                            let mut gap_ends =
                                Vec::with_capacity(remaining_ordering_components.len());
                            for rc in &remaining_ordering_components {
                                if let Some(gs) = helper.graph.get_graphstorage(rc)
                                    && (gs.has_outgoing_edges(s.node).unwrap_or_default()
                                        || gs.has_ingoing_edges(s.node).unwrap_or_default())
                                {
                                    gap_ends.push((rc, s.node));
                                }
                            }
                            gap_ends
                        })
                        .flatten()
                        .collect_vec();
                for (c, n) in reachable_from_left {
                    for (c_, n_) in &reachable_from_right {
                        if &c == c_ {
                            let comp_storage = helper.graph.get_graphstorage(c_);
                            if let Some(gs) = comp_storage {
                                if let Some(old_outgoing) =
                                    gs.get_outgoing_edges(n).flatten().next()
                                {
                                    update.add_event(UpdateEvent::DeleteNode {
                                        node_name: helper.node_name(old_outgoing)?,
                                    })?;
                                }
                                if let Some(old_incoming) =
                                    gs.get_ingoing_edges(*n_).flatten().next()
                                {
                                    update.add_event(UpdateEvent::DeleteNode {
                                        node_name: helper.node_name(old_incoming)?,
                                    })?;
                                }
                            }
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: helper.node_name(n)?,
                                target_node: helper.node_name(*n_)?,
                                layer: c.layer.to_string(),
                                component_type: c.get_type().to_string(),
                                component_name: c.name.to_string(),
                            })?;
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: helper.node_name(*n_)?,
                                target_node: self.source_stem.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::PartOf.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                    }
                }
            }
            let mut new_tok_sequence_end = None;
            if let Some(res_m2) = m2
                && let Some(res_m3) = m3
            {
                let link_source = res_m2
                    .get(1)
                    .ok_or(anyhow!("Result cannot be parsed."))?
                    .node;
                new_tok_sequence_end = Some(link_source);
                let link_target = res_m3
                    .get(1)
                    .ok_or(anyhow!("Result cannot be parsed."))?
                    .node;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(link_source)?,
                    target_node: helper.node_name(link_target)?,
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(link_source)?,
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                let reachable_from_left =
                    CycleSafeDFS::new_inverse(&coverage_container, link_source, 1, usize::MAX)
                        .into_iter()
                        .flatten()
                        .map(|s| {
                            let mut gap_starts =
                                Vec::with_capacity(remaining_ordering_components.len());
                            for rc in &remaining_ordering_components {
                                if let Some(gs) = helper.graph.get_graphstorage(rc)
                                    && (gs.has_outgoing_edges(s.node).unwrap_or_default()
                                        || gs.has_ingoing_edges(s.node).unwrap_or_default())
                                {
                                    gap_starts.push((rc, s.node));
                                }
                            }
                            gap_starts
                        })
                        .flatten()
                        .collect_vec();
                let reachable_from_right =
                    CycleSafeDFS::new_inverse(&coverage_container, link_target, 1, usize::MAX)
                        .into_iter()
                        .flatten()
                        .map(|s| {
                            let mut gap_ends =
                                Vec::with_capacity(remaining_ordering_components.len());
                            for rc in &remaining_ordering_components {
                                if let Some(gs) = helper.graph.get_graphstorage(rc)
                                    && (gs.has_outgoing_edges(s.node).unwrap_or_default()
                                        || gs.has_ingoing_edges(s.node).unwrap_or_default())
                                {
                                    gap_ends.push((rc, s.node));
                                }
                            }
                            gap_ends
                        })
                        .flatten()
                        .collect_vec();
                for (c, n) in reachable_from_left {
                    for (c_, n_) in &reachable_from_right {
                        if &c == c_ {
                            let comp_storage = helper.graph.get_graphstorage(c_);
                            if let Some(gs) = comp_storage {
                                if let Some(old_outgoing) =
                                    gs.get_outgoing_edges(n).flatten().next()
                                {
                                    update.add_event(UpdateEvent::DeleteNode {
                                        node_name: helper.node_name(old_outgoing)?,
                                    })?;
                                }
                                if let Some(old_incoming) =
                                    gs.get_ingoing_edges(*n_).flatten().next()
                                {
                                    update.add_event(UpdateEvent::DeleteNode {
                                        node_name: helper.node_name(old_incoming)?,
                                    })?;
                                }
                            }
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: helper.node_name(n)?,
                                target_node: helper.node_name(*n_)?,
                                layer: c.layer.to_string(),
                                component_type: c.get_type().to_string(),
                                component_name: c.name.to_string(),
                            })?;
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: helper.node_name(n)?,
                                target_node: self.source_stem.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::PartOf.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                    }
                }
            }
            if let Some(start_tok_node_id) = new_tok_sequence_start
                && let Some(end_tok_node_id) = new_tok_sequence_end
            {
                let span_name = format!(
                    "{}#op-span_{start_tok_node_id}-{end_tok_node_id}",
                    self.source_stem
                );
                update.add_event(UpdateEvent::AddNode {
                    node_name: span_name.to_string(),
                    node_type: "node".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: span_name.to_string(),
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                let mut connected_nodes = default_ordering_gs
                    .find_connected(start_tok_node_id, 0, std::ops::Bound::Unbounded)
                    .flatten();
                while let Some(next_tok_node) = connected_nodes.next()
                    && next_tok_node != end_tok_node_id
                {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: span_name.to_string(),
                        target_node: helper.node_name(next_tok_node)?,
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                update.add_event(UpdateEvent::AddEdge {
                    source_node: span_name.to_string(),
                    target_node: helper.node_name(end_tok_node_id)?,
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        {
            // update last node_id (insert_node_at looks back -1, so this might be relevant)
            if let Some(v) = self
                .source_nodes
                .get_mut(old_index + old_len.unwrap_or(1) - 1)
            {
                *v = end_of_insertion_sequence;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
