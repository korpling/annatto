use std::collections::BTreeSet;

use graphannis::{
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::{
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::Edge,
};
use itertools::Itertools;
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError,
    progress::ProgressReporter,
    workflow::{StatusMessage, StatusSender},
    Module, StepID,
};

use super::Manipulator;

#[derive(Deserialize)]
pub struct Collapse {
    ctype: AnnotationComponentType,
    layer: String,
    name: String,
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
            let source_nodes = component_storage.source_nodes().collect_vec();
            let progress = ProgressReporter::new(
                tx,
                StepID {
                    module_name: MODULE_NAME.to_string(),
                    path: None,
                },
                source_nodes.len(),
            )?;
            for sn in source_nodes {
                let source_node = sn?;
                for et in component_storage.get_outgoing_edges(source_node) {
                    let edge_target = et?;
                    self.collapse_single_edge(source_node, edge_target, graph, &mut update)?;
                }
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

    fn collapse_single_edge(
        &self,
        source_node: u64,
        target_node: u64,
        graph: &mut AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.transfer_node_annos(&source_node, &target_node, graph, update)?;
        self.reconnect_components(&source_node, &target_node, graph, update)?;
        Ok(())
    }

    fn transfer_node_annos(
        &self,
        from_node: &u64,
        to_node: &u64,
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
        let to_node_name = node_annos
            .get_value_for_item(to_node, &NODE_NAME_KEY)?
            .unwrap();
        for key in node_annos.get_all_keys_for_item(from_node, None, None)? {
            if key.ns != ANNIS_NS {
                let anno_value = node_annos
                    .get_value_for_item(from_node, key.as_ref())?
                    .unwrap(); // if that panics, graphannis broke
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: to_node_name.to_string(),
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
        to_node: &u64,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
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
            let mut update_edges = BTreeSet::new();
            for tn in storage.get_outgoing_edges(*from_node) {
                let target_node = tn?;
                update_edges.insert(Edge {
                    source: *from_node,
                    target: target_node,
                });
            }
            for esrc in storage.source_nodes() {
                let edge_source = esrc?;
                for rn in storage.get_outgoing_edges(edge_source) {
                    let reachable_node = rn?;
                    if &reachable_node == from_node {
                        update_edges.insert(Edge {
                            source: edge_source,
                            target: reachable_node,
                        });
                    }
                }
            }
            for edge in update_edges {
                self.update_edge(&edge, from_node, to_node, graph, &component, update)?;
            }
        }
        Ok(())
    }

    fn update_edge(
        &self,
        edge: &Edge,
        replace: &u64,
        replace_with: &u64,
        graph: &AnnotationGraph,
        component: &AnnotationComponent,
        update: &mut GraphUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let node_annos = graph.get_node_annos();
        let (new_source_name, new_target_name) = if &edge.source == replace {
            (
                node_annos
                    .get_value_for_item(replace_with, &NODE_NAME_KEY)?
                    .unwrap(),
                node_annos
                    .get_value_for_item(&edge.target, &NODE_NAME_KEY)?
                    .unwrap(),
            )
        } else {
            (
                node_annos
                    .get_value_for_item(&edge.source, &NODE_NAME_KEY)?
                    .unwrap(),
                node_annos
                    .get_value_for_item(replace_with, &NODE_NAME_KEY)?
                    .unwrap(),
            )
        };
        update.add_event(UpdateEvent::DeleteEdge {
            source_node: node_annos
                .get_value_for_item(&edge.source, &NODE_NAME_KEY)?
                .unwrap()
                .to_string(),
            target_node: node_annos
                .get_value_for_item(&edge.target, &NODE_NAME_KEY)?
                .unwrap()
                .to_string(),
            layer: component.layer.to_string(),
            component_type: component.get_type().to_string(),
            component_name: component.name.to_string(),
        })?;
        update.add_event(UpdateEvent::AddEdge {
            source_node: new_source_name.to_string(),
            target_node: new_target_name.to_string(),
            layer: component.layer.to_string(),
            component_type: component.get_type().to_string(),
            component_name: component.name.to_string(),
        })?;
        if let Some(storage) = graph.get_graphstorage(component) {
            let anno_storage = storage.get_anno_storage();
            for anno_key in anno_storage.get_all_keys_for_item(edge, None, None)? {
                if anno_key.ns != ANNIS_NS {
                    if let Some(anno_value) =
                        anno_storage.get_value_for_item(edge, anno_key.as_ref())?
                    {
                        update.add_event(UpdateEvent::AddEdgeLabel {
                            source_node: new_source_name.to_string(),
                            target_node: new_target_name.to_string(),
                            layer: component.layer.to_string(),
                            component_type: component.get_type().to_string(),
                            component_name: component.name.to_string(),
                            anno_ns: anno_key.ns.to_string(),
                            anno_name: anno_key.name.to_string(),
                            anno_value: anno_value.to_string(),
                        })?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, path::Path};

    use graphannis::{
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::graph::ANNIS_NS;
    use tempfile::tempdir_in;

    use crate::manipulator::Manipulator;

    use super::Collapse;

    #[test]
    fn test_collapse_in_mem() {
        let r = test(false);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    #[test]
    fn test_collapse_on_disk() {
        let r = test(true);
        assert!(r.is_ok(), "Test in mem failed: {:?}", r.err());
    }

    fn test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut g = input_graph(on_disk)?;
        let collapse = Collapse {
            ctype: AnnotationComponentType::Pointing,
            layer: "".to_string(),
            name: "align".to_string(),
        };
        assert!(collapse
            .manipulate_corpus(&mut g, Path::new("./"), None)
            .is_ok());
        let mut expected_g = target_graph(on_disk)?;
        let db_dir = tempdir_in(temp_dir())?;
        let g_dir = tempdir_in(&db_dir)?;
        let e_dir = tempdir_in(&db_dir)?;
        g.save_to(g_dir.path().join("current").as_path())?;
        expected_g.save_to(e_dir.path().join("current").as_path())?;
        let cs_e = CorpusStorage::with_auto_cache_size(e_dir.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(g_dir.path(), true)?;
        let queries = [
            ("node .a node", 4),
            ("node .b node", 5),
            ("node ->align node", 0),
            ("node ->dep node", 3),
            ("node > node", 7),
        ];
        for (query_str, n) in queries {
            let query_e = SearchQuery {
                corpus_names: &["current"],
                query: query_str,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let query_g = SearchQuery {
                corpus_names: &["current"],
                query: query_str,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query_e, 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query_g, 0, None, ResultOrder::Normal)?;
            assert_eq!(n, matches_e.len(), "Query failed: {}", query_str);
            assert_eq!(matches_e, matches_g, "Query failed: {}", query_str);
        }
        Ok(())
    }

    fn input_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
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
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("{corpus}#b{i}"),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{corpus}#a{i}"),
                target_node: format!("{corpus}#b{i}"),
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
            source_node: format!("{corpus}#a4"),
            target_node: format!("{corpus}#b5"),
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

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
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
        for i in 0..12 {
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
