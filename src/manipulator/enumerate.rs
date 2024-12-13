use std::collections::BTreeSet;

use documented::{Documented, DocumentedFields};
use graphannis::{
    corpusstorage::{QueryLanguage, SearchQuery},
    graph::AnnoKey,
    update::{GraphUpdate, UpdateEvent},
    CorpusStorage,
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use tempfile::tempdir;

use crate::{
    deserialize::deserialize_anno_key, error::AnnattoError, progress::ProgressReporter, StepID,
};

use super::Manipulator;

/// Adds a node label to all matched nodes for set of queries with the number of
/// the match as value.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct EnumerateMatches {
    /// A list of queries to find the nodes that are to be enumerated.
    queries: Vec<String>,
    /// The target node in the query that is assigned the numeric annotation. Holds for all queries. This is a 1-based index and counts by mention in the query.
    target: usize,
    /// The anno key of the numeric annotation that should be created.
    /// Example:
    /// ```toml
    /// [export.config]
    /// label = { ns = "order", name = "i" }
    /// ```
    ///
    /// You can also provide this as a string:
    /// ```toml
    /// [export.config]
    /// label = "order::i"
    /// ```
    #[serde(default = "default_label", deserialize_with = "deserialize_anno_key")]
    label: AnnoKey,
    /// An optional 1-based index pointing to the annotation node in the query that holds a prefix value that will be added to the numeric annotation.
    value: Option<usize>,
    /// This can be used to offset the numeric values in the annotations.
    start: u64,
}

fn default_label() -> AnnoKey {
    AnnoKey {
        name: "i".into(),
        ns: "".into(),
    }
}

impl Default for EnumerateMatches {
    fn default() -> Self {
        Self {
            queries: vec!["node".to_string()],
            target: 1,
            label: default_label(),
            value: None,
            start: 0,
        }
    }
}

impl Manipulator for EnumerateMatches {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        {
            let corpus_name = "enumerate-this";
            let tmp_dir = tempdir()?;
            graph.save_to(&tmp_dir.path().join(corpus_name))?;
            let cs = CorpusStorage::with_auto_cache_size(tmp_dir.path(), true)?;
            let progress = ProgressReporter::new(tx, step_id.clone(), self.queries.len())?;
            for query_s in &self.queries {
                let query = SearchQuery {
                    corpus_names: &[corpus_name],
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
                            if let Some(value_i) = self.value {
                                let mut coords = m.split(' ').collect_vec();
                                if value_i <= coords.len() {
                                    let coord = coords.remove(value_i - 1);
                                    let (coord_ns, coord_name, coord_node_name) =
                                        match coord.rsplit_once("::") {
                                            Some((anno, node_name)) => {
                                                let (ns, anno_name) =
                                                    anno.split_once("::").unwrap_or(("", anno));
                                                (ns, anno_name, node_name)
                                            }
                                            None => (ANNIS_NS, "tok", coord),
                                        };
                                    if let Some(internal_id) = graph
                                        .get_node_annos()
                                        .get_node_id_from_name(coord_node_name)?
                                    {
                                        if let Some(prefix) =
                                            graph.get_node_annos().get_value_for_item(
                                                &internal_id,
                                                &AnnoKey {
                                                    ns: coord_ns.into(),
                                                    name: coord_name.into(),
                                                },
                                            )?
                                        {
                                            update.add_event(UpdateEvent::AddNodeLabel {
                                                node_name: target_node.to_string(),
                                                anno_ns: self.label.ns.to_string(),
                                                anno_name: self.label.name.to_string(),
                                                anno_value: format!(
                                                    "{prefix}-{}",
                                                    i as u64 + self.start - offset
                                                ),
                                            })?;
                                        }
                                    }
                                }
                            } else {
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: target_node.to_string(),
                                    anno_ns: self.label.ns.to_string(),
                                    anno_name: self.label.name.to_string(),
                                    anno_value: (i as u64 + self.start - offset).to_string(),
                                })?;
                            }
                            visited.insert(target_node.to_string());
                        }
                    } else {
                        return Err(Box::new(AnnattoError::Manipulator {
                            reason: format!(
                                "No matching node with index {} for query {query_s}",
                                &self.target
                            ),
                            manipulator: step_id.module_name.clone(),
                        }));
                    }
                }
                progress.worked(1)?;
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
    use insta::assert_snapshot;
    use itertools::Itertools;

    use crate::{
        exporter::graphml::GraphMLExporter,
        manipulator::Manipulator,
        test_util::{compare_results, export_to_string},
        StepID,
    };

    use super::EnumerateMatches;

    #[test]
    fn bare_enumerate_in_mem() {
        let r = enumerate_bare(false);
        assert!(r.is_ok(), "Error testing enumerate in mem: {:?}", r.err());
    }

    #[test]
    fn bare_enumerate_on_disk() {
        let r = enumerate_bare(true);
        assert!(r.is_ok(), "Error testing enumerate on disk: {:?}", r.err());
    }

    #[test]
    fn prefixed_enumerate_in_mem() {
        let r = enumerate_with_value(false);
        assert!(r.is_ok(), "Error testing enumerate in mem: {:?}", r.err());
    }

    #[test]
    fn prefixed_enumerate_on_disk() {
        let r = enumerate_with_value(true);
        assert!(r.is_ok(), "Error testing enumerate on disk: {:?}", r.err());
    }

    fn enumerate_bare(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
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
            label: AnnoKey {
                name: "i".into(),
                ns: "count".into(),
            },
            queries: vec!["annis:node_type=\"node\"".to_string()],
            target: 1,
            start: 1,
            value: None,
        };
        let step_id = StepID {
            module_name: "manipulate".to_string(),
            path: None,
        };
        manipulate.manipulate_corpus(&mut input_g, Path::new("who_cares"), step_id, None)?;
        let expected_annos = expected_g.get_node_annos();
        let output_annos = input_g.get_node_annos();
        let mut expected_matches = expected_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        expected_matches.sort_unstable_by(compare_results);

        let mut output_matches = output_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        output_matches.sort_unstable_by(compare_results);

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
        if on_disk {
            let actual = export_to_string(&input_g, GraphMLExporter::default());
            assert!(actual.is_ok());
            assert_snapshot!(actual.unwrap());
        }
        Ok(())
    }

    fn enumerate_with_value(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut input_g = base_graph(on_disk)?;
        let mut expected_g = base_graph(on_disk)?;
        let mut u = GraphUpdate::default();
        for (i, val) in ["positive", "negative", "neutral"].iter().enumerate() {
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("corpus/document#t{}", i + 1),
                anno_ns: "count".to_string(),
                anno_name: "i".to_string(),
                anno_value: format!("{val}-{}", i + 1),
            })?;
        }
        expected_g.apply_update(&mut u, |_| {})?;
        let manipulate = EnumerateMatches {
            label: AnnoKey {
                name: "i".into(),
                ns: "count".into(),
            },
            queries: vec!["sentiment".to_string()],
            target: 1,
            start: 1,
            value: Some(1),
        };
        let step_id = StepID {
            module_name: "manipulate".to_string(),
            path: None,
        };
        manipulate.manipulate_corpus(&mut input_g, Path::new("who_cares"), step_id, None)?;
        let expected_annos = expected_g.get_node_annos();
        let output_annos = input_g.get_node_annos();
        let mut expected_matches = expected_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        expected_matches.sort_unstable_by(compare_results);

        let mut output_matches = output_annos
            .exact_anno_search(Some("count"), "i", ValueSearch::Any)
            .collect_vec();
        output_matches.sort_unstable_by(compare_results);

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
        if on_disk {
            let actual = export_to_string(&input_g, GraphMLExporter::default());
            assert!(actual.is_ok());
            assert_snapshot!(actual.unwrap());
        }
        Ok(())
    }

    fn base_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
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
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t1".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment".to_string(),
            anno_value: "positive".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document#t2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t2".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment".to_string(),
            anno_value: "negative".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/document#t3".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t3".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment".to_string(),
            anno_value: "neutral".to_string(),
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
