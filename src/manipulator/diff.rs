use std::{borrow::Cow, collections::BTreeMap, usize};

use anyhow::anyhow;
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, NodeID},
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
pub struct MarkDiffs {
    /// Provide an annotation key that distinguishes relevant sub graphs to match
    /// differences between. Default is `annis::doc`, which means that diffs are
    /// annotated by comparing documents with the same name in different subgraphs.
    #[serde(default = "default_by_key")]
    by: AnnoKey,
    #[serde(with = "crate::estarde::annotation_component")]
    source_component: AnnotationComponent,
    #[serde(with = "crate::estarde::anno_key")]
    source_key: AnnoKey,
    #[serde(with = "crate::estarde::annotation_component")]
    target_component: AnnotationComponent,
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

impl Manipulator for MarkDiffs {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let pairs = self.pair(graph, &progress)?;
        let progress = ProgressReporter::new(tx.clone(), step_id, pairs.len())?;
        for pair in pairs {
            pair.diff(
                graph,
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

impl MarkDiffs {
    fn pair(
        &self,
        graph: &AnnotationGraph,
        progress: &ProgressReporter,
    ) -> Result<Vec<SequencePair>, anyhow::Error> {
        let node_annos = graph.get_node_annos();
        let by_values = node_annos.get_all_values(&self.by, false)?;
        let source_storage = graph
            .get_graphstorage(&self.source_component)
            .ok_or(anyhow!("Source component cannot be found."))?;
        let target_storage = graph
            .get_graphstorage(&self.target_component)
            .ok_or(anyhow!("Target component cannot be found."))?;
        let part_of_storages = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .into_iter()
            .map(|c| graph.get_graphstorage(&c))
            .flatten()
            .collect_vec();
        let part_of_container = UnionEdgeContainer::new(
            part_of_storages
                .iter()
                .map(|s| s.as_edgecontainer())
                .collect_vec(),
        );
        let mut sequence_pairs = Vec::with_capacity(by_values.len());
        for value in by_values {
            dbg!(&value);
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
            let source_parent = matching[0].node;
            let mut source_dfs =
                CycleSafeDFS::new_inverse(&part_of_container, source_parent, 0, usize::MAX)
                    .flatten();
            let mut random_source_node = None;
            while let Some(s) = source_dfs.next() {
                let n = s.node;
                if source_storage.has_ingoing_edges(n)? || source_storage.has_outgoing_edges(n)? {
                    random_source_node = Some(n);
                    break;
                }
            }
            let ordered_node = random_source_node.ok_or(anyhow!(
                "No ordered source node found via part of components."
            ))?;
            let mut ordered_source_nodes = source_storage
                .find_connected_inverse(ordered_node, 0, std::ops::Bound::Unbounded)
                .flatten()
                .collect_vec();
            ordered_source_nodes.reverse();
            ordered_source_nodes.extend(
                source_storage
                    .find_connected(ordered_node, 1, std::ops::Bound::Unbounded)
                    .flatten(),
            );
            let target_parent = matching[1].node;
            let mut target_dfs =
                CycleSafeDFS::new_inverse(&part_of_container, target_parent, 0, usize::MAX)
                    .flatten();
            let mut random_target_node = None;
            while let Some(s) = target_dfs.next() {
                let n = s.node;
                if target_storage.has_ingoing_edges(n)? || target_storage.has_outgoing_edges(n)? {
                    random_target_node = Some(n);
                    break;
                }
            }
            let ordered_node = random_target_node.ok_or(anyhow!(
                "No ordered source node found via part of components."
            ))?;
            let mut ordered_target_nodes = source_storage
                .find_connected_inverse(ordered_node, 0, std::ops::Bound::Unbounded)
                .flatten()
                .collect_vec();
            ordered_target_nodes.reverse();
            ordered_target_nodes.extend(
                target_storage
                    .find_connected(ordered_node, 1, std::ops::Bound::Unbounded)
                    .flatten(),
            );
            let seq_pair = SequencePair {
                source_nodes: ordered_source_nodes,
                target_nodes: ordered_target_nodes,
            };
            sequence_pairs.push(seq_pair);
        }
        Ok(sequence_pairs)
    }
}

struct SequencePair {
    source_nodes: Vec<NodeID>,
    target_nodes: Vec<NodeID>,
}

impl SequencePair {
    fn diff(
        self,
        graph: &AnnotationGraph,
        source_key: &AnnoKey,
        target_key: &AnnoKey,
        algorithm: DiffAlgorithm,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        let mut vocab = graph.get_node_annos().get_all_values(source_key, false)?;
        vocab.extend(graph.get_node_annos().get_all_values(target_key, false)?);
        let vocab = vocab
            .into_iter()
            .enumerate()
            .map(|(i, v)| (v, i))
            .collect::<BTreeMap<Cow<str>, usize>>(); // TODO make this an argument
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
                    if let Some(v) = graph.get_node_annos().get_value_for_item(n, key)?
                        && let Some(index) = vocab.get(&v)
                    {
                        value_target.push(*index);
                    }
                }
            }
            (a, b)
        };
        let diffs = capture_diff_slices(algorithm.into(), &a, &b);
        dbg!(&a);
        dbg!(&b);
        dbg!(&diffs);
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
                    for (src_node, tgt_node) in self.source_nodes[old_index..old_index + old_len]
                        .iter()
                        .zip_eq(&self.target_nodes[new_index..new_index + new_len])
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
                            anno_value: "sub".to_string(),
                        })?;
                    }
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
        manipulator::{Manipulator, diff::MarkDiffs},
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
        let d: Result<MarkDiffs, _> = toml::from_str(
            r#"
        source_component = { ctype = "Ordering", layer = "annis", name = "dipl" }
        source_key = "dipl::dipl"
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
}
