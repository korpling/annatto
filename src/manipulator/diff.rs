use std::{borrow::Cow, collections::BTreeMap, sync::Arc, usize};

use anyhow::{anyhow, bail};
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, EdgeContainer, GraphStorage, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, storage::union::UnionEdgeContainer},
};
use itertools::Itertools;
use likewise::{Algorithm, DiffOp, capture_diff_slices};
use serde::{Deserialize, Serialize};

use crate::{manipulator::Manipulator, progress::ProgressReporter, util::update_graph_silent};

/// Compare to sub graphs, derive a patch from one towards the other,
/// and apply it.
#[derive(Deserialize, Serialize)]
pub struct DiffSubgraphs {
    /// Provide an annotation key that distinguishes relevant sub graphs to match
    /// differences between. Default is `annis::doc`, which means that diffs are
    /// annotated by comparing documents with the same name in different subgraphs.
    #[serde(default = "default_by_key")]
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
}

fn default_by_key() -> AnnoKey {
    AnnoKey {
        ns: ANNIS_NS.into(),
        name: "doc".into(),
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum DiffAlgorithm {
    Lcs,
    Myers,
    #[default]
    Patience,
}

impl Into<Algorithm> for DiffAlgorithm {
    fn into(self) -> Algorithm {
        match self {
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
        let graph_helper =
            GraphDiffHelper::new(graph, &self.source_component, &self.target_component)?;
        let pairs = self.pair(graph_helper, &progress)?;
        let progress = ProgressReporter::new(tx.clone(), step_id, pairs.len())?;
        for pair in pairs {
            pair.diff(
                graph,
                Vocabulary::new(graph, &[self.source_key.clone(), self.target_key.clone()])?,
                &self.source_key,
                &self.target_key,
                self.algorithm.clone(),
                &mut update,
            )?;
            progress.worked(1)?;
        }
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

struct GraphDiffHelper<'a> {
    graph: &'a AnnotationGraph,
    part_of_storages: Vec<Arc<dyn GraphStorage>>,
    gstore_a: Arc<dyn GraphStorage>,
    gstore_b: Arc<dyn GraphStorage>,
}

impl<'a> GraphDiffHelper<'a> {
    fn new(
        graph: &'a AnnotationGraph,
        component_a: &AnnotationComponent,
        component_b: &AnnotationComponent,
    ) -> Result<Self, anyhow::Error> {
        let gstore_a = graph
            .get_graphstorage(component_a)
            .ok_or(anyhow!("Source component storage does not exist."))?;
        let gstore_b = graph
            .get_graphstorage(component_b)
            .ok_or(anyhow!("Target component storage does not exist."))?;
        let part_of_storages = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .into_iter()
            .map(|c| graph.get_graphstorage(&c))
            .flatten()
            .collect_vec();
        Ok(GraphDiffHelper {
            graph,
            part_of_storages,
            gstore_a,
            gstore_b,
        })
    }

    fn node_name(&self, node: NodeID) -> Result<Cow<'a, str>, anyhow::Error> {
        self.graph
            .get_node_annos()
            .get_value_for_item(&node, &NODE_NAME_KEY)?
            .ok_or(anyhow!("Node has no name."))
    }

    fn get_parent(&self, node: NodeID) -> Result<Cow<'a, str>, anyhow::Error> {
        let container = UnionEdgeContainer::new(
            self.part_of_storages
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
        let part_of_container = UnionEdgeContainer::new(
            self.part_of_storages
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect(),
        );
        let mut dfs =
            CycleSafeDFS::new_inverse(&part_of_container, parent, 0, usize::MAX).flatten();
        let mut random_seq_node = None;
        while let Some(s) = dfs.next() {
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
            self.sequence(self.gstore_a.clone(), source_parent)?,
            self.sequence(self.gstore_b.clone(), target_parent)?,
        ))
    }
}

impl<'a> DiffSubgraphs {
    fn pair(
        &self,
        graph_helper: GraphDiffHelper<'a>,
        progress: &ProgressReporter,
    ) -> Result<Vec<SequencePair<'a>>, anyhow::Error> {
        let node_annos = graph_helper.graph.get_node_annos();
        let by_values = node_annos.get_all_values(&self.by, false)?;
        let mut sequence_pairs = Vec::with_capacity(by_values.len());
        for value in by_values {
            let matching = node_annos
                .exact_anno_search(Some(&self.by.ns), &self.by.name, ValueSearch::Some(&value))
                .flatten()
                .collect_vec();
            if matching.len() != 2 {
                progress.warn(&format!(
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

struct Vocabulary<'a> {
    dictionary: BTreeMap<Cow<'a, str>, usize>,
}

impl<'a> Vocabulary<'a> {
    fn new(graph: &'a AnnotationGraph, keys: &[AnnoKey]) -> Result<Self, anyhow::Error> {
        let mut dict = BTreeMap::new();
        for k in keys {
            let values = graph.get_node_annos().get_all_values(k, false)?;
            values.into_iter().for_each(|v| {
                if !dict.contains_key(&v) {
                    dict.insert(v, dict.len());
                }
            });
        }
        Ok(Vocabulary { dictionary: dict })
    }

    fn to_index(&self, value: Cow<str>) -> Result<usize, anyhow::Error> {
        self.dictionary
            .get(&value)
            .map(|i| *i)
            .ok_or(anyhow!("Unknown value."))
    }
}

struct SequencePair<'a> {
    source_stem: Cow<'a, str>,
    target_stem: Cow<'a, str>,
    source_nodes: Vec<NodeID>,
    target_nodes: Vec<NodeID>,
}

impl<'a> SequencePair<'a> {
    fn diff(
        self,
        graph: &'a AnnotationGraph,
        vocab: Vocabulary,
        source_key: &AnnoKey,
        target_key: &AnnoKey,
        algorithm: DiffAlgorithm,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
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
                    if let Some(v) = graph.get_node_annos().get_value_for_item(n, key)? {
                        value_target.push(vocab.to_index(v)?);
                    }
                }
            }
            (a, b)
        };
        let diffs = capture_diff_slices(algorithm.into(), &a, &b);
        for d in diffs {
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
                        let old_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(src_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
                        let new_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(tgt_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
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
                        let old_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(node_id, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
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
                        let new_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(node_id, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
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
                        let src_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(src_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
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
                        let tgt_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(tgt_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
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
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        StepID,
        exporter::graphml::GraphMLExporter,
        importer::{Importer, exmaralda::ImportEXMARaLDA},
        manipulator::{Manipulator, diff::DiffSubgraphs},
        test_util::export_to_string,
        util::update_graph_silent,
    };

    #[test]
    fn diff() {
        let import: Result<ImportEXMARaLDA, _> = toml::from_str("");
        assert!(import.is_ok());
        let import = import.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/diff"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(update_graph_silent(&mut graph, &mut update).is_ok());
        assert!(graph.calculate_all_statistics().is_ok());
        let d: Result<DiffSubgraphs, _> = toml::from_str(
            r#"
        source_parent = "diff/a"
        source_component = { ctype = "Ordering", layer = "annis", name = "dipl" }
        source_key = "dipl::dipl"
        target_parent = "diff/b"
        target_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        target_key = "norm::norm"
        "#,
        );
        assert!(d.is_ok());
        let diff = d.unwrap();
        assert!(
            diff.manipulate_corpus(
                &mut graph,
                Path::new("./"),
                StepID {
                    module_name: "test_manip".to_string(),
                    path: None
                },
                None
            )
            .is_ok()
        );
        let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(export.is_ok());
        let export = export.unwrap();
        let actual = export_to_string(&graph, export);
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn diff_inverse() {
        let import: Result<ImportEXMARaLDA, _> = toml::from_str("");
        assert!(import.is_ok());
        let import = import.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/diff"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(update_graph_silent(&mut graph, &mut update).is_ok());
        assert!(graph.calculate_all_statistics().is_ok());
        let d: Result<DiffSubgraphs, _> = toml::from_str(
            r#"
        target_parent = "diff/a"
        target_component = { ctype = "Ordering", layer = "annis", name = "dipl" }
        target_key = "dipl::dipl"
        source_parent = "diff/b"
        source_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        source_key = "norm::norm"
        "#,
        );
        assert!(d.is_ok());
        let diff = d.unwrap();
        let manip = diff.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_manip".to_string(),
                path: None,
            },
            None,
        );
        assert!(manip.is_ok(), "Err: {:?}", manip.err().unwrap());
        let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(export.is_ok());
        let export = export.unwrap();
        let actual = export_to_string(&graph, export);
        assert_snapshot!(actual.unwrap());
    }
}
