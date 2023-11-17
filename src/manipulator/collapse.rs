use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use graphannis::{
    graph::GraphStorage,
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::Edge,
};
use itertools::Itertools;
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError,
    progress::ProgressReporter,
    workflow::{StatusMessage, StatusSender},
    Module,
};

use super::Manipulator;

#[derive(Deserialize)]
pub struct Collapse {
    ctype: AnnotationComponentType,
    layer: String,
    name: String,
    #[serde(default)]
    disjoint: bool, // performance boost -> if you know all edges are already disjoint, an expensive step can be skipped
}

const MODULE_NAME: &str = "collapse_component";

impl Module for Collapse {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Manipulator for Collapse {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = self.collapse(graph, tx)?;
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

type EdgeUId = (u64, u64, AnnotationComponent);

impl Collapse {
    fn collapse(
        &self,
        graph: &mut AnnotationGraph,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let component = AnnotationComponent::new(
            self.ctype.clone(),
            (&self.layer).into(),
            (&self.name).into(),
        );
        if let Some(component_storage) = graph.get_graphstorage(&component) {
            let hyperedges = self.collect_hyperedges(component_storage, tx.clone())?;
            let mut hypernode_map = BTreeMap::new();
            for (id, hyperedge) in hyperedges.iter().enumerate() {
                for m in hyperedge {
                    let name = format!("hypernode#{id}");
                    update.add_event(UpdateEvent::AddNode {
                        node_name: name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    hypernode_map.insert(m, name);
                    let node_name = graph
                        .get_node_annos()
                        .get_value_for_item(m, &NODE_NAME_KEY)?
                        .unwrap();
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: node_name.to_string(),
                    })?;
                }
            }
            // collapse hyperedges
            let progress = ProgressReporter::new(tx, self.step_id(None), hyperedges.len())?;
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
        } else if let Some(sender) = &tx {
            let msg = StatusMessage::Failed(AnnattoError::Manipulator {
                reason: format!(
                    "No component {}::{}::{} found.",
                    component.get_type(),
                    component.layer,
                    component.name
                ),
                manipulator: MODULE_NAME.to_string(),
            });
            sender.send(msg)?;
        }
        Ok(update)
    }

    fn collect_hyperedges(
        &self,
        component_storage: Arc<dyn GraphStorage>,
        tx: Option<StatusSender>,
    ) -> Result<Vec<BTreeSet<u64>>, Box<dyn std::error::Error>> {
        let mut hyperedges = Vec::new();
        let source_nodes = component_storage.source_nodes().collect_vec();
        let progress = ProgressReporter::new(tx.clone(), self.step_id(None), source_nodes.len())?;
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
            let progress_disjoint =
                ProgressReporter::new_unknown_total_work(tx, self.step_id(None))?;
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
        let random_node = hyperedge.iter().last().unwrap();
        let target_node_name = hypernode_map.get(random_node).unwrap();
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
        let from_node_name = node_annos
            .get_value_for_item(from_node, &NODE_NAME_KEY)?
            .unwrap();
        update.add_event(UpdateEvent::DeleteNode {
            node_name: from_node_name.to_string(),
        })?;
        for key in node_annos.get_all_keys_for_item(from_node, None, None)? {
            if key.ns != ANNIS_NS {
                let anno_value = node_annos
                    .get_value_for_item(from_node, key.as_ref())?
                    .unwrap(); // if that panics, graphannis broke
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: to_node.to_string(),
                    anno_ns: key.ns.to_string(),
                    anno_name: key.name.to_string(),
                    anno_value: anno_value.to_string(),
                })?;
            }
        }
        Ok(())
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
            if component.get_type() == self.ctype
                && component.layer == self.layer
                && component.name == self.name
            {
                // ignore component that is to be deleted
                continue;
            }
            let storage = graph.get_graphstorage(&component).unwrap();
            let annos = storage.get_anno_storage();
            for tn in storage.get_outgoing_edges(*from_node) {
                let target_node = tn?;
                let edge_id: EdgeUId = (*from_node, target_node, component.clone());
                if skip_edges.contains(&edge_id) {
                    continue;
                }
                let new_target_node_name = match hypernode_map.get(&target_node) {
                    Some(name) => name.to_string(),
                    None => graph
                        .get_node_annos()
                        .get_value_for_item(&target_node, &NODE_NAME_KEY)?
                        .unwrap()
                        .to_string(),
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
                    let anno_val = annos.get_value_for_item(&edge, &anno_key)?.unwrap();
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
                skip_edges.insert(edge_id);
            }
            for sn in storage.get_ingoing_edges(*from_node) {
                let source_node = sn?;
                let edge_id: EdgeUId = (source_node, *from_node, component.clone());
                if skip_edges.contains(&edge_id) {
                    continue;
                }
                let new_source_node_name = match hypernode_map.get(&source_node) {
                    Some(name) => name.to_string(),
                    None => graph
                        .get_node_annos()
                        .get_value_for_item(&source_node, &NODE_NAME_KEY)?
                        .unwrap()
                        .to_string(),
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
                    let anno_val = annos.get_value_for_item(&edge, &anno_key)?.unwrap();
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
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use itertools::Itertools;
    use serde_derive::Deserialize;

    use crate::{
        manipulator::{check::Check, Manipulator},
        workflow::StatusMessage,
    };

    use super::Collapse;

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
        let r = test(false, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_on_disk() {
        let r = test(true, false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_in_mem() {
        let r = test(false, true);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_disjoint_on_disk() {
        let r = test(true, true);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    fn test(on_disk: bool, disjoint: bool) -> Result<(), Box<dyn std::error::Error>> {
        let g_ = input_graph(on_disk, disjoint);
        assert!(g_.is_ok());
        let mut g = g_?;
        let collapse = Collapse {
            ctype: AnnotationComponentType::Pointing,
            layer: "".to_string(),
            name: "align".to_string(),
            disjoint,
        };
        let (msg_sender, msg_receiver) = mpsc::channel();
        assert!(collapse
            .manipulate_corpus(&mut g, Path::new("./"), Some(msg_sender))
            .is_ok());
        assert!(msg_receiver.into_iter().count() > 0);
        let eg = target_graph(on_disk, disjoint);
        assert!(eg.is_ok());
        let mut expected_g = eg?;
        let toml_str_r = if disjoint {
            fs::read_to_string("tests/data/graph_op/collapse/test_check_disjoint.toml")
        } else {
            fs::read_to_string("tests/data/graph_op/collapse/test_check.toml")
        };
        assert!(toml_str_r.is_ok());
        let toml_str = toml_str_r?;
        let check_r: Result<Check, _> = toml::from_str(toml_str.as_str());
        assert!(check_r.is_ok());
        let check = check_r?;
        let dummy_path = Path::new("./");
        let (sender_e, receiver_e) = mpsc::channel();
        let r = check.manipulate_corpus(&mut expected_g, dummy_path, Some(sender_e));
        assert!(r.is_ok());
        let mut failed_tests = receiver_e
            .into_iter()
            .filter(|m| matches!(m, StatusMessage::Failed { .. }))
            .collect_vec();
        if !failed_tests.is_empty() {
            if let Some(StatusMessage::Failed(e)) = failed_tests.pop() {
                return Err(Box::new(e));
            }
        }
        let (sender, receiver) = mpsc::channel();
        let cr = check.manipulate_corpus(&mut g, dummy_path, Some(sender));
        assert!(cr.is_ok());
        failed_tests = receiver
            .into_iter()
            .filter(|m| matches!(m, StatusMessage::Failed { .. }))
            .collect_vec();
        if !failed_tests.is_empty() {
            if let Some(StatusMessage::Failed(e)) = failed_tests.pop() {
                return Err(Box::new(e));
            }
        }
        Ok(())
    }

    fn input_graph(
        on_disk: bool,
        disjoint: bool,
    ) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::new(on_disk)?;
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
        let mut g = AnnotationGraph::new(on_disk)?;
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
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("{corpus}#a{i}"),
                anno_ns: "b".to_string(),
                anno_name: "anno".to_string(),
                anno_value: "b".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#a{i}"),
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
            }
        }
        // syntax
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#a1"),
            target_node: format!("{corpus}#a0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#a1"),
            target_node: format!("{corpus}#a0"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "det".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#a2"),
            target_node: format!("{corpus}#a1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#a2"),
            target_node: format!("{corpus}#a1"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "subj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#a2"),
            target_node: format!("{corpus}#a4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#a2"),
            target_node: format!("{corpus}#a4"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: "func".to_string(),
            anno_value: "comp:obj".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{corpus}#a4"),
            target_node: format!("{corpus}#a3"),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: format!("{corpus}#a4"),
            target_node: format!("{corpus}#a3"),
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
        let max_t = if disjoint { 10 } else { 12 };
        for i in 0..max_t {
            let r = i / 2;
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#t{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#a{r}"),
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
