use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    usize,
};

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
use linked_hash_set::LinkedHashSet;
use serde::{Deserialize, Serialize};

use crate::{
    manipulator::Manipulator,
    progress::ProgressReporter,
    util::{update_graph, update_graph_silent},
};

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
        let diffs = pairs
            .iter()
            .map(|p| {
                compute_diff(
                    &p.source_nodes,
                    &p.target_nodes,
                    &graph_helper,
                    &self.source_key,
                    &self.target_key,
                    self.algorithm.clone(),
                )
            })
            .flatten()
            .collect_vec();
        let total_num_of_diff_ops = diffs.iter().map(|d| d.len()).sum();
        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), total_num_of_diff_ops)?;
        progress.info(format!(
            "Applying {total_num_of_diff_ops} diff operations across {} sequence pair(s) ...",
            pairs.len()
        ))?;
        let orderings_existing_before = graph_helper
            .graph()
            .get_all_components(Some(AnnotationComponentType::Ordering), None);
        let mut licensed_tok_nodes: LinkedHashSet<NodeID> = LinkedHashSet::new();
        for (pair, diff) in pairs.into_iter().zip_eq(diffs) {
            if self.merge {
                licensed_tok_nodes.extend(&pair.merge_diff(
                    &mut graph_helper,
                    diff,
                    &mut update,
                    &progress,
                )?);
            } else {
                pair.annotate_diff(&mut graph_helper, &mut update, diff, &progress)?;
            }
        }
        if self.merge {
            for oc in orderings_existing_before {
                let gs = graph_helper.graph.get_or_create_writable(&oc)?;
                gs.clear()?;
            }
        }
        progress.info("Applying first update ...")?;
        update_graph(&mut graph_helper.graph, &mut update, Some(step_id), tx)?;
        graph_helper.graph.calculate_all_statistics()?;
        progress.info("Cleaning up ...")?;
        update = GraphUpdate::default();
        let query = aql::parse("node_type=/node/ !@* node_type=/corpus/?", false)?;
        for m in aql::execute_query_on_graph(graph_helper.graph(), &query, true, None)?.flatten() {
            if let Some(Match { node, .. }) = m.get(0) {
                update.add_event(UpdateEvent::DeleteNode {
                    node_name: graph_helper.node_name(*node)?,
                })?;
            }
        }
        let query = aql::parse("tok", false)?;
        for m in aql::execute_query_on_graph(graph_helper.graph(), &query, true, None)?.flatten() {
            if let Some(Match { node, .. }) = m.get(0)
                && !licensed_tok_nodes.contains(node)
            {
                update.add_event(UpdateEvent::DeleteNode {
                    node_name: graph_helper.node_name(*node)?,
                })?;
            }
        }
        progress.info(format!("Clean-up update has size {}", update.len()?))?;
        update_graph_silent(graph_helper.graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
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

fn compute_diff(
    source_nodes: &[NodeID],
    target_nodes: &[NodeID],
    helper: &GraphDiffHelper,
    source_key: &AnnoKey,
    target_key: &AnnoKey,
    algorithm: DiffAlgorithm,
) -> Result<Vec<DiffOp>, anyhow::Error> {
    let mut vocabulary = BTreeMap::default();
    let (a, b) = {
        let (mut a, mut b) = (
            Vec::with_capacity(source_nodes.len()),
            Vec::with_capacity(target_nodes.len()),
        );
        for ((node_id_src, value_target), key) in [source_nodes, target_nodes]
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

impl SequencePair {
    fn annotate_diff(
        self,
        helper: &mut GraphDiffHelper,
        update: &mut GraphUpdate,
        diff: Vec<DiffOp>,
        progress: &ProgressReporter,
    ) -> Result<(), anyhow::Error> {
        for d in diff {
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
            progress.worked(1)?;
        }
        helper.apply_update(update)?;
        Ok(())
    }

    fn merge_diff(
        self,
        helper: &mut GraphDiffHelper,
        diff: Vec<DiffOp>,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
    ) -> Result<LinkedHashSet<NodeID>, anyhow::Error> {
        update.add_event(UpdateEvent::DeleteNode {
            node_name: self.target_stem.to_string(),
        })?;
        let n_expected_elements = self.source_nodes.len().max(self.target_nodes.len());
        let mut new_tok_order: LinkedHashSet<NodeID> =
            LinkedHashSet::with_capacity(n_expected_elements);
        let l_gs = helper
            .graph()
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::LeftToken,
                ANNIS_NS.to_string(),
                "".to_string(),
            ))
            .ok_or(anyhow!("Could not get left token component storage."))?;
        let r_gs = helper
            .graph()
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::RightToken,
                ANNIS_NS.to_string(),
                "".to_string(),
            ))
            .ok_or(anyhow!("Could not get right token component storage."))?;
        let default_ordering_gs = helper
            .graph()
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.to_string(),
                "".to_string(),
            ))
            .ok_or(anyhow!("Could not obtain storage of default ordering"))?;
        let mut vertical_storages = helper
            .graph()
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .into_iter()
            .map(|c| helper.graph().get_graphstorage(&c))
            .flatten()
            .collect_vec();
        let dominance_storages = helper
            .graph()
            .get_all_components(Some(AnnotationComponentType::Dominance), None)
            .into_iter()
            .map(|c| helper.graph().get_graphstorage(&c))
            .flatten()
            .collect_vec();
        vertical_storages.extend(dominance_storages);
        let vertical_container = UnionEdgeContainer::new(
            vertical_storages
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect_vec(),
        );
        progress.info("Evaluating diffs ...")?;
        for d in diff {
            match d {
                DiffOp::Equal {
                    old_index,
                    len,
                    new_index,
                } => {
                    let start_node = l_gs
                        .find_connected(self.source_nodes[old_index], 0, std::ops::Bound::Unbounded)
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find left most token."))?;
                    let end_node = r_gs
                        .find_connected(
                            self.source_nodes[old_index + len - 1],
                            0,
                            std::ops::Bound::Unbounded,
                        )
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find right most token."))?;
                    let mut order_it = default_ordering_gs
                        .find_connected(start_node, 0, std::ops::Bound::Unbounded)
                        .flatten();
                    while let Some(node_id) = order_it.next() {
                        new_tok_order.insert(node_id);
                        if node_id == end_node {
                            break;
                        }
                    }
                    for node_id in &self.target_nodes[new_index..new_index + len - 1] {
                        for DFSStep {
                            node: downward_reachable,
                            ..
                        } in CycleSafeDFS::new(&vertical_container, *node_id, 0, usize::MAX)
                            .flatten()
                        {
                            update.add_event(UpdateEvent::DeleteNode {
                                node_name: helper.node_name(downward_reachable)?,
                            })?;
                            for DFSStep {
                                node: upward_reachable,
                                ..
                            } in CycleSafeDFS::new_inverse(
                                &vertical_container,
                                downward_reachable,
                                1,
                                usize::MAX,
                            )
                            .flatten()
                            {
                                // soft delete
                                update.add_event(UpdateEvent::DeleteEdge {
                                    source_node: helper.node_name(upward_reachable)?,
                                    target_node: self.target_stem.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::PartOf.to_string(),
                                    component_name: "".to_string(),
                                })?;
                            }
                        }
                    }
                }
                DiffOp::Delete {
                    old_index, old_len, ..
                } => {
                    for node_id in &self.source_nodes[old_index..old_index + old_len - 1] {
                        for DFSStep {
                            node: downward_reachable,
                            ..
                        } in CycleSafeDFS::new(&vertical_container, *node_id, 0, usize::MAX)
                            .flatten()
                        {
                            update.add_event(UpdateEvent::DeleteNode {
                                node_name: helper.node_name(downward_reachable)?,
                            })?;
                            for DFSStep {
                                node: upward_reachable,
                                ..
                            } in CycleSafeDFS::new_inverse(
                                &vertical_container,
                                downward_reachable,
                                1,
                                usize::MAX,
                            )
                            .flatten()
                            {
                                // soft delete
                                update.add_event(UpdateEvent::DeleteEdge {
                                    source_node: helper.node_name(upward_reachable)?,
                                    target_node: self.source_stem.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::PartOf.to_string(),
                                    component_name: "".to_string(),
                                })?;
                            }
                        }
                    }
                }
                DiffOp::Insert {
                    new_index, new_len, ..
                } => {
                    let start_node = l_gs
                        .find_connected(self.target_nodes[new_index], 0, std::ops::Bound::Unbounded)
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find left most token."))?;
                    let end_node = r_gs
                        .find_connected(
                            self.target_nodes[new_index + new_len - 1],
                            0,
                            std::ops::Bound::Unbounded,
                        )
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find right most token."))?;
                    let mut order_it = default_ordering_gs
                        .find_connected(start_node, 0, std::ops::Bound::Unbounded)
                        .flatten();
                    let mut integrate_nodes = BTreeSet::default();
                    while let Some(node_id) = order_it.next() {
                        new_tok_order.insert(node_id);
                        for DFSStep { node, .. } in
                            CycleSafeDFS::new_inverse(&vertical_container, node_id, 1, usize::MAX)
                                .flatten()
                        {
                            integrate_nodes.insert(node);
                        }
                        if node_id == end_node {
                            break;
                        }
                    }
                    for node_id in integrate_nodes {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: helper.node_name(node_id)?,
                            target_node: self.source_stem.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                }
                DiffOp::Replace {
                    new_index,
                    new_len,
                    old_index,
                    old_len,
                } => {
                    let start_node = l_gs
                        .find_connected(self.target_nodes[new_index], 0, std::ops::Bound::Unbounded)
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find left most token."))?;
                    let end_node = r_gs
                        .find_connected(
                            self.target_nodes[new_index + new_len - 1],
                            0,
                            std::ops::Bound::Unbounded,
                        )
                        .flatten()
                        .last()
                        .ok_or(anyhow!("Could not find right most token."))?;
                    let mut order_it = default_ordering_gs
                        .find_connected(start_node, 0, std::ops::Bound::Unbounded)
                        .flatten();
                    while let Some(node_id) = order_it.next()
                        && node_id != end_node
                    {
                        new_tok_order.insert(node_id);
                    }
                    new_tok_order.insert(end_node);
                    for node_id in &self.source_nodes[old_index..old_index + old_len - 1] {
                        for DFSStep {
                            node: downward_reachable,
                            ..
                        } in CycleSafeDFS::new(&vertical_container, *node_id, 0, usize::MAX)
                            .flatten()
                        {
                            update.add_event(UpdateEvent::DeleteNode {
                                node_name: helper.node_name(downward_reachable)?,
                            })?;
                            for DFSStep {
                                node: upward_reachable,
                                ..
                            } in CycleSafeDFS::new_inverse(
                                &vertical_container,
                                downward_reachable,
                                1,
                                usize::MAX,
                            )
                            .flatten()
                            {
                                // soft delete
                                update.add_event(UpdateEvent::DeleteEdge {
                                    source_node: helper.node_name(upward_reachable)?,
                                    target_node: self.source_stem.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::PartOf.to_string(),
                                    component_name: "".to_string(),
                                })?;
                            }
                        }
                    }
                }
            }
            progress.worked(1)?;
        }
        progress.info("Rebuilding secondary orderings ...")?;
        let mut ordering_map = BTreeMap::<AnnotationComponent, LinkedHashSet<NodeID>>::default();
        for oc in helper
            .graph()
            .get_all_components(Some(AnnotationComponentType::Ordering), None)
        {
            ordering_map.insert(oc, LinkedHashSet::with_capacity(n_expected_elements));
        }
        for (source_node, target_node) in new_tok_order.iter().tuple_windows() {
            update.add_event(UpdateEvent::AddEdge {
                source_node: helper.node_name(*source_node)?,
                target_node: helper.node_name(*target_node)?,
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
            // first the ending nodes, then the starting nodes
            for lookup_storage in [&r_gs, &l_gs] {
                for covering_node in lookup_storage
                    .find_connected_inverse(*source_node, 1, std::ops::Bound::Included(1))
                    .flatten()
                {
                    for (oc, ordered_set) in ordering_map.iter_mut() {
                        let gs = helper.graph().get_graphstorage(oc).ok_or(anyhow!(
                            "Component {oc} does not provide a readable storage."
                        ))?;
                        if (gs.has_ingoing_edges(covering_node)?
                            || gs.has_outgoing_edges(covering_node)?)
                            && !ordered_set.contains(&covering_node)
                        {
                            ordered_set.insert(covering_node);
                        }
                    }
                }
            }
        }
        // the last target node was never probed for starting or ending nodes,
        // we only check for ending ones
        if let Some(node_id) = new_tok_order.iter().next_back() {
            for covering_node in r_gs
                .find_connected_inverse(*node_id, 1, std::ops::Bound::Included(1))
                .flatten()
            {
                for (oc, ordered_set) in ordering_map.iter_mut() {
                    let gs = helper.graph().get_graphstorage(oc).ok_or(anyhow!(
                        "Component {oc} does not provide a readable storage."
                    ))?;
                    if (gs.has_ingoing_edges(covering_node)?
                        || gs.has_outgoing_edges(covering_node)?)
                        && !ordered_set.contains(&covering_node)
                    {
                        ordered_set.insert(covering_node);
                    }
                }
            }
        }
        progress.info("Building new primary ordering ...")?;
        new_tok_order.iter().try_for_each(|n| {
            update.add_event(UpdateEvent::AddEdge {
                source_node: helper.node_name(*n)?,
                target_node: self.source_stem.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            update.add_event(UpdateEvent::DeleteEdge {
                source_node: helper.node_name(*n)?,
                target_node: self.target_stem.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            Ok::<(), anyhow::Error>(())
        })?;
        progress.info("Building new secondary orderings ...")?;
        for (oc, ordered_set) in ordering_map {
            for (source_node, target_node) in ordered_set.iter().tuple_windows() {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(*source_node)?,
                    target_node: helper.node_name(*target_node)?,
                    layer: oc.layer.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: oc.name.to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(*source_node)?,
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                update.add_event(UpdateEvent::DeleteEdge {
                    source_node: helper.node_name(*source_node)?,
                    target_node: self.target_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if let Some(node_id) = ordered_set.iter().next_back() {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: helper.node_name(*node_id)?,
                    target_node: self.source_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                update.add_event(UpdateEvent::DeleteEdge {
                    source_node: helper.node_name(*node_id)?,
                    target_node: self.target_stem.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        Ok(new_tok_order)
    }
}

#[cfg(test)]
mod tests;
