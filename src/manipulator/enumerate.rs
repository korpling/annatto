use std::{collections::BTreeSet, env::temp_dir};

use graphannis::{
    corpusstorage::{QueryLanguage, SearchQuery},
    update::{GraphUpdate, UpdateEvent},
    CorpusStorage,
};
use itertools::Itertools;
use serde_derive::Deserialize;
use tempfile::tempdir_in;

use crate::{error::AnnattoError, Module};

use super::Manipulator;

#[derive(Deserialize)]
#[serde(default)]
pub struct EnumerateMatches {
    queries: Vec<String>,
    target: usize,
    label_ns: String,
    label_name: String,
    start: u128,
}

impl Default for EnumerateMatches {
    fn default() -> Self {
        Self {
            queries: vec!["node".to_string()],
            target: 1,
            label_ns: "".to_string(),
            label_name: "i".to_string(),
            start: 0,
        }
    }
}

const MODULE_NAME: &str = "enumerate_matches";

impl Module for EnumerateMatches {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Manipulator for EnumerateMatches {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let corpus_name = "current";
        let tmp_dir = tempdir_in(temp_dir())?;
        graph.save_to(&tmp_dir.path().join(corpus_name))?;
        let cs = CorpusStorage::with_auto_cache_size(tmp_dir.path(), true)?;
        for query_s in &self.queries {
            let query = SearchQuery {
                corpus_names: &["current"],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let search_results = cs.find(
                query,
                0,
                None,
                graphannis::corpusstorage::ResultOrder::Normal,
            )?;
            let mut offset = 0;
            let mut visited = BTreeSet::new();
            for (i, m) in search_results.into_iter().enumerate() {
                let matching_nodes = m
                    .split(' ')
                    .filter_map(|s| s.split("::").last())
                    .collect_vec();
                if let Some(target_node) = matching_nodes.get(self.target - 1) {
                    if visited.contains(*target_node) {
                        offset += 1;
                    } else {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: target_node.to_string(),
                            anno_ns: self.label_ns.to_string(),
                            anno_name: self.label_name.to_string(),
                            anno_value: (i as u128 + self.start - offset).to_string(),
                        })?;
                        visited.insert(target_node.to_string());
                    }
                } else {
                    return Err(Box::new(AnnattoError::Manipulator {
                        reason: format!(
                            "No matching node with index {} for query {query_s}",
                            &self.target
                        ),
                        manipulator: MODULE_NAME.to_string(),
                    }));
                }
            }
        }
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS, types::AnnoKey};
    use itertools::Itertools;

    use crate::manipulator::Manipulator;

    use super::EnumerateMatches;

    #[test]
    fn test_enumerate_in_mem() {
        let r = test_enumerate(false);
        assert!(r.is_ok(), "Error testing enumerate in mem: {:?}", r.err());
    }

    #[test]
    fn test_enumerate_on_disk() {
        let r = test_enumerate(true);
        assert!(r.is_ok(), "Error testing enumerate on disk: {:?}", r.err());
    }

    fn test_enumerate(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut input_g = base_graph(on_disk)?;
        let mut expected_g = base_graph(on_disk)?;
        let mut u = GraphUpdate::default();
        for i in 1..4 {
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("corpus/document#t{i}"),
                anno_ns: "count".to_string(),
                anno_name: "i".to_string(),
                anno_value: i.to_string(),
            })?;
        }
        expected_g.apply_update(&mut u, |_| {})?;
        let manipulate = EnumerateMatches {
            label_name: "i".to_string(),
            label_ns: "count".to_string(),
            queries: vec!["annis:node_type=\"node\"".to_string()],
            target: 1,
            start: 1,
        };
        manipulate.manipulate_corpus(&mut input_g, Path::new("who_cares"), None)?;
        let expected_annos = expected_g.get_node_annos();
        let output_annos = input_g.get_node_annos();
        let expected_matches = expected_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        let output_matches = output_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        assert_eq!(expected_matches.len(), output_matches.len());
        let anno_key = AnnoKey {
            ns: "count".into(),
            name: "i".into(),
        };
        for (em, om) in expected_matches.into_iter().zip(output_matches) {
            let enode = em?.node;
            let onode = om?.node;
            let evalue = expected_annos.get_value_for_item(&enode, &anno_key)?;
            let ovalue = output_annos.get_value_for_item(&onode, &anno_key)?;
            assert!(evalue.is_some());
            assert!(ovalue.is_some());
            assert_eq!(evalue.unwrap(), ovalue.unwrap());
        }
        Ok(())
    }

    fn base_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document".to_string(),
            target_node: "corpus".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document#t1".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document#t2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document#t3".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document#t1".to_string(),
            target_node: "corpus/document#t2".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document#t2".to_string(),
            target_node: "corpus/document#t3".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document#t1".to_string(),
            target_node: "corpus/document".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document#t2".to_string(),
            target_node: "corpus/document".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/document#t3".to_string(),
            target_node: "corpus/document".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}