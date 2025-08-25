use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::GraphStorage,
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME, NODE_NAME_KEY},
    types::Edge,
};
use itertools::Itertools;
use serde::Serialize;
use serde_derive::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    StepID, error::AnnattoError, progress::ProgressReporter, util::update_graph_silent,
    workflow::StatusSender,
};

use super::Manipulator;

/// Collapse an edge component,
///
/// Given a component, this graph operation joins source and target node of each
/// edge to a single node. This could be done by keeping one of the nodes or by
/// creating a third one. Then all all edges, annotations, etc. are moved to the
/// node of choice, the other node(s) is/are deleted.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Collapse {
    /// The component type within which to find the edges to collapse.
    #[serde(with = "crate::estarde::annotation_component")]
    component: AnnotationComponent,
    /// If you know that any two edges in the defined component are always pairwise disjoint, set this attribute to true to save computation time.
    #[serde(default)]
    disjoint: bool, // performance boost -> if you know all edges are already disjoint, an expensive step can be skipped
    /// if true, the node name of the edge terminals defines the node name of nodes resulting from collapsed hyperedges
    #[serde(default)]
    keep_name: bool,
}

impl Manipulator for Collapse {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &tx {
            sender.send(crate::workflow::StatusMessage::Info(
                "Starting to collapse".to_string(),
            ))?;
        }
        let mut update = self.collapse(graph, &step_id, tx)?;
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

type EdgeUId = (u64, u64, AnnotationComponent);

fn parent_node(
    graph: &AnnotationGraph,
    node_id: &u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let component =
        AnnotationComponent::new(AnnotationComponentType::PartOf, ANNIS_NS.into(), "".into());
    let parent_name = if let Some(storage) = graph.get_graphstorage(&component) {
        let mut out_edges = storage.get_outgoing_edges(*node_id).collect_vec();
        if out_edges.len() != 1 {
            "".to_string()
        } else {
            let parent = out_edges.remove(0)?;
            if let Some(name) = graph
                .get_node_annos()
                .get_value_for_item(&parent, &NODE_NAME_KEY)?
            {
                name.to_string()
            } else {
                "".to_string()
            }
        }
    } else {
        "".to_string()
    };
    Ok(parent_name)
}

const HYPERNODE_NAME_STEM: &str = "#mrg";

impl Collapse {
    fn collapse(
        &self,
        graph: &mut AnnotationGraph,
        step_id: &StepID,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let component = &self.component;
        if let Some(component_storage) = graph.get_graphstorage(component) {
            let hyperedges = self.collect_hyperedges(&component_storage, step_id, tx.clone())?;
            let mut hypernode_map = BTreeMap::new();
            let offset = graph
                .get_node_annos()
                .regex_anno_search(
                    Some(ANNIS_NS),
                    NODE_NAME,
                    format!(".*{HYPERNODE_NAME_STEM}.*").as_str(),
                    false,
                )
                .count();
            let progress_create_nodes =
                ProgressReporter::new(tx.clone(), step_id.clone(), hyperedges.len())?;
            progress_create_nodes.info("Starting to build hypernodes")?;
            for (mut id, hyperedge) in hyperedges.iter().enumerate() {
                id += offset;
                let mut parent_to_member = BTreeMap::default();
                for (parent, member) in hyperedge
                    .iter()
                    .map(|m| (parent_node(graph, m).unwrap_or_default(), m))
                {
                    let member_name = graph
                        .get_node_annos()
                        .get_value_for_item(member, &NODE_NAME_KEY)?
                        .unwrap_or_default()
                        .to_string();
                    let relevant_suffix = member_name
                        .split('#')
                        .next_back()
                        .unwrap_or_default()
                        .to_string();
                    parent_to_member
                        .entry(parent)
                        .or_insert(Vec::with_capacity(hyperedge.len()))
                        .push(relevant_suffix);
                }
                let (hypernode_name, _) = parent_to_member
                    .into_iter()
                    .reduce(|a, b| {
                        let mut v = Vec::with_capacity(a.1.len() + b.1.len() + 3);
                        v.push(a.0);
                        v.extend(a.1);
                        v.push("".to_string());
                        v.push(b.0);
                        v.extend(b.1);
                        (v.join("_"), vec![])
                    })
                    .unwrap_or_default();
                let trace_name = if self.keep_name {
                    // if keep name is true, the first terminal node is picked and its name is used
                    if let Some(name_giving_node) = hyperedge.iter().find(|n| {
                        !component_storage
                            .has_outgoing_edges(**n)
                            .unwrap_or_default()
                    }) {
                        graph
                            .get_node_annos()
                            .get_value_for_item(name_giving_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Could not determine name for resulting node."))?
                            .to_string()
                    } else {
                        return Err(anyhow!(
                            "Could not determine node to provide name for collapsed hypernode"
                        )
                        .into());
                    }
                } else {
                    format!("{HYPERNODE_NAME_STEM}{id}_{hypernode_name}")
                };
                update.add_event(UpdateEvent::AddNode {
                    node_name: trace_name.to_string(),
                    node_type: "node".to_string(),
                })?;
                for m in hyperedge {
                    hypernode_map.insert(m, trace_name.to_string());
                    if let Some(node_name) = graph
                        .get_node_annos()
                        .get_value_for_item(m, &NODE_NAME_KEY)?
                    {
                        if self.keep_name && node_name == trace_name {
                            continue;
                        }
                        update.add_event(UpdateEvent::DeleteNode {
                            node_name: node_name.to_string(),
                        })?;
                    } else {
                        return Err(AnnattoError::Manipulator {
                            reason: format!(
                                "Node {m} has no node name or it cannot be retrieved, this can lead to an invalid result."
                            ),
                            manipulator: step_id.module_name.to_string(),
                        })?;
                    }
                }
                progress_create_nodes.worked(1)?;
            }
            // collapse hyperedges
            let progress = ProgressReporter::new(tx, step_id.clone(), hyperedges.len())?;
            progress.info("Starting to join nodes")?;
            let mut processed_edges = BTreeSet::new();
            for hyperedge in &hyperedges {
                self.collapse_hyperedge(
                    hyperedge,
                    &hypernode_map,
                    graph,
                    &mut update,
                    &mut processed_edges,
                )?;
                progress.worked(1)?;
            }
        } else {
            return Err(AnnattoError::Manipulator {
                reason: format!(
                    "No component {}::{}::{} found.",
                    component.get_type(),
                    component.layer,
                    component.name
                ),
                manipulator: step_id.module_name.clone(),
            }
            .into());
        }
        Ok(update)
    }

    fn collect_hyperedges(
        &self,
        component_storage: &Arc<dyn GraphStorage>,
        step_id: &StepID,
        tx: Option<StatusSender>,
    ) -> Result<Vec<BTreeSet<u64>>, Box<dyn std::error::Error>> {
        let source_nodes = component_storage.source_nodes().collect_vec();
        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), source_nodes.len())?;
        progress.info("Starting to collect hyperedges")?;
        let mut hyperedges = Vec::with_capacity(source_nodes.len() / 2); // this should not grow for most use cases
        for sn in source_nodes {
            let source_node = sn?;
            let dfs = CycleSafeDFS::new(
                component_storage.as_edgecontainer(),
                source_node,
                1,
                usize::MAX,
            );
            let mut hyperedge: BTreeSet<u64> = dfs
                .filter_map(|stepr| match stepr {
                    Err(_) => None,
                    Ok(s) => Some(s.node),
                })
                .collect();
            hyperedge.insert(source_node);
            hyperedges.push(hyperedge);
            progress.worked(1)?;
        }
        if !self.disjoint {
            // make sure hyperedges are disjoint
            let progress_disjoint = ProgressReporter::new_unknown_total_work(tx, step_id.clone())?;
            progress_disjoint.info("Starting to build disjoint hyperedges")?;
            let mut repeat = true;
            while repeat {
                let mut disjoint_hyperedges = Vec::new();
                let mut skip = BTreeSet::new();
                let n = hyperedges.len();
                for (i, query_edge) in hyperedges.iter().enumerate() {
                    if skip.contains(&i) {
                        continue;
                    }
                    let mut joint_edge = query_edge.clone();
                    for (j, probe_edge) in (hyperedges[i..]).iter().enumerate() {
                        if joint_edge.intersection(probe_edge).count() > 0 {
                            joint_edge.extend(probe_edge);
                            skip.insert(j + i);
                        }
                    }
                    disjoint_hyperedges.push(joint_edge);
                }
                repeat = n > disjoint_hyperedges.len();
                hyperedges = disjoint_hyperedges;
                progress_disjoint.worked(1)?;
            }
        }
        Ok(hyperedges)
    }

    fn collapse_hyperedge(
        &self,
        hyperedge: &BTreeSet<u64>,
        hypernode_map: &BTreeMap<&u64, String>,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
        skip_edges: &mut BTreeSet<EdgeUId>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let random_node = if let Some(n) = hyperedge.iter().next_back() {
            n
        } else {
            return Err(Box::new(AnnattoError::Manipulator {
                reason: "Encountered an empty hyperedge.".to_string(),
                manipulator: "collapse::hyperedge".to_string(),
            }));
        };
        let target_node_name = if let Some(node_id) = hypernode_map.get(random_node) {
            node_id
        } else {
            return Err(Box::new(AnnattoError::Manipulator {
                reason: "Hypernode is unknown.".to_string(),
                manipulator: "collapse::hyperedge".to_string(),
            }));
        };
        for node_id in hyperedge {
            self.transfer_node_annos(node_id, target_node_name, graph, update)?;
            self.reconnect_components(
                node_id,
                target_node_name,
                hypernode_map,
                graph,
                update,
                skip_edges,
            )?;
        }
        Ok(())
    }

    fn transfer_node_annos(
        &self,
        from_node: &u64,
        to_node: &str,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let node_annos = graph.get_node_annos();
        let from_node_name = if let Some(v) =
            node_annos.get_value_for_item(from_node, &NODE_NAME_KEY)?
        {
            v
        } else {
            return Err(anyhow!("Original node has no name. This is a severe error that originates somewhere in the graph model.").into());
        };
        if to_node == from_node_name {
            return Ok(());
        }
        update.add_event(UpdateEvent::DeleteNode {
            node_name: from_node_name.to_string(),
        })?;
        if let Some(component_storage) = graph.get_graphstorage(&self.component) {
            for key in node_annos.get_all_keys_for_item(from_node, None, None)? {
                // only transfer `annis::` annotations for terminal nodes of the component and never transfer the node name key
                if (key.ns.as_str() != ANNIS_NS
                    || !component_storage.has_outgoing_edges(*from_node)?)
                    && key != *NODE_NAME_KEY
                    && let Some(anno_value) =
                        node_annos.get_value_for_item(from_node, key.as_ref())?
                {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: to_node.to_string(),
                        anno_ns: key.ns.to_string(),
                        anno_name: key.name.to_string(),
                        anno_value: anno_value.to_string(),
                    })?;
                }
            }
            Ok(())
        } else {
            Err(anyhow!("Could not obtain storage of component {:?}, which is required to determine node status.", &self.component).into())
        }
    }

    fn reconnect_components(
        &self,
        from_node: &u64,
        to_node: &str,
        hypernode_map: &BTreeMap<&u64, String>,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
        skip_edges: &mut BTreeSet<EdgeUId>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for component in graph.get_all_components(None, None) {
            if component == self.component {
                // ignore component that is to be deleted
                continue;
            }
            let storage = if let Some(strg) = graph.get_graphstorage(&component) {
                strg
            } else {
                return Err(anyhow!("Component {component} has no storage.").into());
            };
            let annos = storage.get_anno_storage();
            for tn in storage.get_outgoing_edges(*from_node) {
                let target_node = tn?;
                let edge_id: EdgeUId = (*from_node, target_node, component.clone());
                if skip_edges.contains(&edge_id) {
                    continue;
                }
                let new_target_node_name = if let Some(name) = hypernode_map.get(&target_node) {
                    name.to_string()
                } else if let Some(v) = graph
                    .get_node_annos()
                    .get_value_for_item(&target_node, &NODE_NAME_KEY)?
                {
                    v.to_string()
                } else {
                    return Err(anyhow!("Could not determine hypernode name.").into());
                };
                update.add_event(UpdateEvent::AddEdge {
                    source_node: to_node.to_string(),
                    target_node: new_target_node_name.to_string(),
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                })?;
                let edge = Edge {
                    source: *from_node,
                    target: target_node,
                };
                for anno_key in annos.get_all_keys_for_item(&edge, None, None)? {
                    if let Some(anno_val) = annos.get_value_for_item(&edge, &anno_key)? {
                        update.add_event(UpdateEvent::AddEdgeLabel {
                            source_node: to_node.to_string(),
                            target_node: new_target_node_name.to_string(),
                            layer: component.layer.to_string(),
                            component_type: component.get_type().to_string(),
                            component_name: component.name.to_string(),
                            anno_ns: anno_key.ns.to_string(),
                            anno_name: anno_key.name.to_string(),
                            anno_value: anno_val.to_string(),
                        })?;
                    }
                }
                skip_edges.insert(edge_id);
            }
            for sn in storage.get_ingoing_edges(*from_node) {
                let source_node = sn?;
                let edge_id: EdgeUId = (source_node, *from_node, component.clone());
                if skip_edges.contains(&edge_id) {
                    continue;
                }
                let new_source_node_name = if let Some(name) = hypernode_map.get(&source_node) {
                    name.to_string()
                } else if let Some(v) = graph
                    .get_node_annos()
                    .get_value_for_item(&source_node, &NODE_NAME_KEY)?
                {
                    v.to_string()
                } else {
                    return Err(anyhow!("Could not determine hypernode name.").into());
                };
                update.add_event(UpdateEvent::AddEdge {
                    source_node: new_source_node_name.to_string(),
                    target_node: to_node.to_string(),
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                })?;
                let edge = Edge {
                    source: source_node,
                    target: *from_node,
                };
                for anno_key in annos.get_all_keys_for_item(&edge, None, None)? {
                    if let Some(anno_val) = annos.get_value_for_item(&edge, &anno_key)? {
                        update.add_event(UpdateEvent::AddEdgeLabel {
                            source_node: new_source_node_name.to_string(),
                            target_node: to_node.to_string(),
                            layer: component.layer.to_string(),
                            component_type: component.get_type().to_string(),
                            component_name: component.name.to_string(),
                            anno_ns: anno_key.ns.to_string(),
                            anno_name: anno_key.name.to_string(),
                            anno_value: anno_val.to_string(),
                        })?;
                    }
                }
                skip_edges.insert(edge_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::mpsc};

    use graphannis::{
        AnnotationGraph,
        model::{AnnotationComponent, AnnotationComponentType},
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::graph::ANNIS_NS;

    use insta::assert_snapshot;
    use serde_derive::Deserialize;

    use crate::{
        StepID,
        exporter::graphml::GraphMLExporter,
        manipulator::{Manipulator, check::Check},
        test_util::export_to_string,
        util::example_generator,
        util::update_graph_silent,
    };

    use super::{Collapse, HYPERNODE_NAME_STEM};

    #[test]
    fn serialize_custom() {
        let module = Collapse {
            component: AnnotationComponent::new(
                AnnotationComponentType::Dominance,
                "".into(),
                "syntax".into(),
            ),
            disjoint: true,
            keep_name: false,
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = Collapse {
            component: AnnotationComponent::new(
                AnnotationComponentType::Coverage,
                ANNIS_NS.into(),
                "".into(),
            ),
            disjoint: false,
            keep_name: false,
        };
        assert!(
            module
                .validate_graph(
                    &mut graph,
                    StepID {
                        module_name: "test".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        assert!(graph.global_statistics.is_none());
    }

    #[test]
    fn test_deser() {
        #[derive(Deserialize)]
        struct Container {
            _graph_op: Vec<Collapse>,
        }
        let sp = fs::read_to_string("tests/data/graph_op/collapse/serialized_pass.toml")
            .map_err(|_| assert!(false))
            .unwrap();
        let pass: Result<Container, _> = toml::from_str(&sp);
        assert!(pass.is_ok(), "{:?}", pass.err());
        let sf = fs::read_to_string("tests/data/graph_op/collapse/serialized_fail.toml")
            .map_err(|_| assert!(false))
            .unwrap();
        let fail: Result<Collapse, _> = toml::from_str(&sf);
        assert!(fail.is_err());
    }

    #[test]
    fn test_collapse_in_mem() {
        let r = test(false, false, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_on_disk() {
        let r = test(true, false, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_in_mem() {
        let r = test(false, true, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_on_disk() {
        let r = test(true, true, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_keep_name_in_mem() {
        let r = test(false, true, true);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_keep_name_on_disk() {
        let r = test(true, true, true);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    fn test(
        on_disk: bool,
        disjoint: bool,
        keep_name: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let g_ = input_graph(on_disk, disjoint);
        assert!(g_.is_ok());
        let mut g = g_.unwrap();
        let collapse = Collapse {
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "align".into(),
            ),
            disjoint,
            keep_name,
        };
        let step_id = StepID {
            module_name: "collapse".to_string(),
            path: None,
        };

        let (msg_sender, msg_receiver) = mpsc::channel();
        let application =
            collapse.manipulate_corpus(&mut g, Path::new("./"), step_id.clone(), Some(msg_sender));
        assert!(application.is_ok(), "not Ok: {:?}", application.err());
        assert!(msg_receiver.into_iter().count() > 0);
        let export_gml: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(export_gml.is_ok());
        let gml = export_to_string(&g, export_gml.unwrap());
        assert!(gml.is_ok());
        if keep_name {
            assert_snapshot!(
                format!(
                    "collapse_{}{}",
                    if on_disk { "on_disk" } else { "in_mem" },
                    if disjoint { "_disjoint" } else { "" }
                ),
                gml.unwrap()
            );
        }
        let eg = target_graph(on_disk, disjoint);
        assert!(eg.is_ok());
        let mut expected_g = eg.unwrap();
        let toml_str_r = if disjoint {
            if keep_name {
                fs::read_to_string("tests/data/graph_op/collapse/test_check_disjoint_keep.toml")
            } else {
                fs::read_to_string("tests/data/graph_op/collapse/test_check_disjoint.toml")
            }
        } else {
            fs::read_to_string("tests/data/graph_op/collapse/test_check.toml")
        };
        assert!(toml_str_r.is_ok());
        let toml_str = toml_str_r.unwrap();
        let check_r: Result<Check, _> = toml::from_str(toml_str.as_str());
        assert!(check_r.is_ok());
        let check = check_r.unwrap();
        let dummy_path = Path::new("./");

        let (sender_e, _receiver_e) = mpsc::channel();
        if let Err(e) =
            check.manipulate_corpus(&mut expected_g, dummy_path, step_id.clone(), Some(sender_e))
        {
            return Err(e);
        }

        let (sender, _receiver) = mpsc::channel();
        let step_id = StepID {
            module_name: "collapse".to_string(),
            path: None,
        };
        if let Err(e) = check.manipulate_corpus(&mut g, dummy_path, step_id, Some(sender)) {
            return Err(e);
        }

        Ok(())
    }

    fn input_graph(
        on_disk: bool,
        disjoint: bool,
    ) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        let corpus = "corpus";
        u.add_event(UpdateEvent::AddNode {
            node_name: corpus.to_string(),
            node_type: corpus.to_string(),
        })?;
        for i in 0..5 {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#a{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("{corpus}#a{i}"),
                anno_ns: "a".to_string(),
                anno_name: "anno".to_string(),
                anno_value: "a".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#b{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("{corpus}#b{i}"),
                anno_ns: "b".to_string(),
                anno_name: "anno".to_string(),
                anno_value: "b".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b{i}"),
                target_node: format!("{corpus}#a{i}"),
                layer: "".to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: "align".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#a{i}"),
                target_node: corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b{i}"),
                target_node: corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 0 {
                let j = i - 1;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{corpus}#a{j}"),
                    target_node: format!("{corpus}#a{i}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "a".to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{corpus}#b{j}"),
                    target_node: format!("{corpus}#b{i}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "b".to_string(),
                })?;
            }
        }
        if !disjoint {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#b5"),
                node_type: "node".to_string(),
            })?; // make second ordering one node longer
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b4"),
                target_node: format!("{corpus}#b5"),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "b".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b5"),
                target_node: format!("{corpus}#a4"),
                layer: "".to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: "align".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b5"),
                target_node: corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
        }
        // syntax
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#b1"),
            target_node: format!("{corpus}#b0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#b1"),
            target_node: format!("{corpus}#b0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "det".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#b2"),
            target_node: format!("{corpus}#b1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#b2"),
            target_node: format!("{corpus}#b1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "subj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#b2"),
            target_node: format!("{corpus}#b4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#b2"),
            target_node: format!("{corpus}#b4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "comp:obj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#b4"),
            target_node: format!("{corpus}#b3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#b4"),
            target_node: format!("{corpus}#b3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "mod".to_string(),
        })?;
        // dominance syntax
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s0"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s1"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{corpus}#s1"),
            anno_ns: "".to_string(),
            anno_name: "cat".to_string(),
            anno_value: "subj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s2"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{corpus}#s2"),
            anno_ns: "".to_string(),
            anno_name: "cat".to_string(),
            anno_value: "obj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s0"),
            target_node: format!("{corpus}#s1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s0"),
            target_node: format!("{corpus}#s2"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s1"),
            target_node: format!("{corpus}#a0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s1"),
            target_node: format!("{corpus}#a1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}#a2"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}#a3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}#a4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        // ordered empty nodes
        for i in 0..12 {
            let r = i / 2;
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#t{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#b{r}"),
                target_node: format!("{corpus}#t{i}"),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 0 {
                let j = i - 1;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{corpus}#t{j}"),
                    target_node: format!("{corpus}#t{i}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }

    fn target_graph(
        on_disk: bool,
        disjoint: bool,
    ) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        let corpus = "corpus";
        u.add_event(UpdateEvent::AddNode {
            node_name: corpus.to_string(),
            node_type: corpus.to_string(),
        })?;
        for i in 0..5 {
            let node_id = format!("{corpus}{HYPERNODE_NAME_STEM}{i}");
            u.add_event(UpdateEvent::AddNode {
                node_name: node_id.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_id.to_string(),
                anno_ns: "a".to_string(),
                anno_name: "anno".to_string(),
                anno_value: "a".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_id.to_string(),
                anno_ns: "b".to_string(),
                anno_name: "anno".to_string(),
                anno_value: "b".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: node_id.to_string(),
                target_node: corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 0 {
                let j = i - 1;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{corpus}{HYPERNODE_NAME_STEM}{j}"),
                    target_node: node_id.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "a".to_string(),
                })?;
            }
        }
        // syntax
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}1"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}1"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "det".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "subj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "comp:obj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}4"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}{HYPERNODE_NAME_STEM}4"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "mod".to_string(),
        })?;
        // dominance syntax
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s0"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s1"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{corpus}#s1"),
            anno_ns: "".to_string(),
            anno_name: "cat".to_string(),
            anno_value: "subj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: format!("{corpus}#s2"),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{corpus}#s2"),
            anno_ns: "".to_string(),
            anno_name: "cat".to_string(),
            anno_value: "obj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s0"),
            target_node: format!("{corpus}#s1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s0"),
            target_node: format!("{corpus}#s2"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s1"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s1"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}2"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#s2"),
            target_node: format!("{corpus}{HYPERNODE_NAME_STEM}4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "constituents".to_string(),
        })?;
        // ordered empty nodes
        let max_t = if disjoint { 10 } else { 12 };
        for i in 0..max_t {
            let r = i / 2;
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#t{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}{HYPERNODE_NAME_STEM}{r}"),
                target_node: format!("{corpus}#t{i}"),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 0 {
                let j = i - 1;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{corpus}#t{j}"),
                    target_node: format!("{corpus}#t{i}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
