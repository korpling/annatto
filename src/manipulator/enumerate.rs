use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use anyhow::{Context, anyhow};
use facet::Facet;
use graphannis::{
    aql,
    graph::{AnnoKey, Component, Match},
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Serialize;
use serde_derive::Deserialize;

use crate::{
    StepID,
    error::AnnattoError,
    progress::ProgressReporter,
    util::update_graph_silent,
    util::{
        sort_matches::SortCache,
        token_helper::{TOKEN_KEY, TokenHelper},
    },
};

use super::Manipulator;

/// Adds a node label to all matched nodes for set of queries with the number of
/// the match as value.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnumerateMatches {
    /// A list of queries to find the nodes that are to be enumerated.
    queries: Vec<String>,
    /// The target node in the query that is assigned the numeric annotation.
    /// Holds for all queries. This is a 1-based index and counts by mention in the query.
    /// E. g., for a query "tok _=_ pos _=_ lemma", a target "2" refers to the node holding
    /// the `pos` annotation, "3" to the node holding the `lemma` annotation. The index picks
    /// a node for EACH result of the query, that is returned, i. e. for the given example query
    /// and a target index 2, each node with a `pos` annotation that overlaps identically with
    /// a `tok` node and a node holding a lemma annotation, is selected as an annotation target.
    target: usize,
    /// First sort by the values of the provided node indices referring to the query. Sorting is stable. The first index ranks higher then the second, an so forth.
    /// Everytime the value or the tuple of values of the selected nodes changes, the count is restartet at the `start` value.
    /// Example:
    /// ```toml
    /// [graph_op.config]
    /// query = "tok _=_ pos=/NN/ @* doc"
    /// by = [3]
    /// ```
    ///
    /// The example sorts the results by the value of doc (the rest is kept stable).
    #[serde(default)]
    by: Vec<usize>,
    /// You can additionally sort results after the default sorting
    /// of search results by providing a list of nodes by which
    /// to sort and a definition of their value type.
    ///
    /// Example (sorts by token value before enumerating):
    /// ```toml
    /// [graph_op.config]
    /// query = "tok _=_ pos @* doc"
    /// sort = [1]
    /// ...
    /// ```
    ///
    /// Example (interpreting a node as numeric in sorting):
    /// ```toml
    /// [graph_op.config]
    /// query = "tok @* page"
    /// sort = [{ numeric = 2 }]
    /// ...
    /// ```
    ///
    #[serde(default)]
    sort: Vec<SortByNode>,
    /// The anno key of the numeric annotation that should be created.
    /// Example:
    /// ```toml
    /// [graph_op.config]
    /// label = { ns = "order", name = "i" }
    /// ```
    ///
    /// You can also provide this as a string:
    /// ```toml
    /// [graph_op.config]
    /// label = "order::i"
    /// ```
    #[serde(default = "default_label", with = "crate::estarde::anno_key")]
    label: AnnoKey,
    /// An optional 1-based index pointing to the annotation node in the query that holds a prefix value that will be added to the numeric annotation.
    #[serde(default)]
    value: Option<usize>,
    /// This can be used to offset the numeric values in the annotations.
    #[serde(default)]
    start: usize,
}

fn default_label() -> AnnoKey {
    AnnoKey {
        name: "i".into(),
        ns: "".into(),
    }
}

#[derive(Facet, Deserialize, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[repr(u8)]
enum SortByNode {
    AsString(usize),
    AsInteger { numeric: usize },
}

impl<'a> SortByNode {
    fn query_index(&self) -> usize {
        match self {
            SortByNode::AsString(n) => *n,
            SortByNode::AsInteger { numeric } => *numeric,
        }
    }

    fn sortable_value(&self, value: Cow<'a, str>) -> Result<SortValue<'a>, anyhow::Error> {
        Ok(match self {
            SortByNode::AsString(_) => SortValue::StringValue(value),
            SortByNode::AsInteger { .. } => {
                SortValue::NumericValue(value.parse::<OrderedFloat<f64>>()?)
            }
        })
    }
}

#[derive(PartialEq, PartialOrd, Eq, Ord)]
enum SortValue<'a> {
    StringValue(Cow<'a, str>),
    NumericValue(OrderedFloat<f64>),
}

impl Default for EnumerateMatches {
    fn default() -> Self {
        Self {
            queries: vec!["node".to_string()],
            by: vec![],
            sort: vec![],
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
            let progress = ProgressReporter::new(tx, step_id.clone(), self.queries.len())?;
            let component_order = Component::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "".into(),
            );

            let gs_order = graph.get_graphstorage(&component_order);
            let mut sort_cache = SortCache::new(gs_order.context("Missing ordering component")?);
            let token_helper = TokenHelper::new(graph)?;

            for query_s in &self.queries {
                let query = aql::parse(query_s, false)?;
                let mut search_results = aql::execute_query_on_graph(graph, &query, true, None)?
                    .flatten()
                    .collect_vec();
                // Sort results with the default ANNIS sort order
                search_results.sort_by(|m1, m2| {
                    sort_cache
                        .compare_matchgroup_by_text_pos(
                            m1,
                            m2,
                            graph.get_node_annos(),
                            &token_helper,
                        )
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                // final sort by by-nodes
                let mut additional_sort_keys = self
                    .by
                    .iter()
                    .map(|n| SortByNode::AsString(*n))
                    .collect_vec();
                additional_sort_keys.extend_from_slice(&self.sort);
                if !additional_sort_keys.is_empty() {
                    let mut parse_err_at = None;
                    search_results.sort_by(|a, b| {
                        let mut values: [Vec<SortValue>; 2] = [Vec::default(), Vec::default()];
                        for (i, data) in [a, b].iter().enumerate() {
                            for by_node in &additional_sort_keys {
                                if let Some(result_member) = data.get(by_node.query_index() - 1) {
                                    let anno_key = anno_key_for_match(result_member);

                                    if let Ok(Some(v)) = graph
                                        .get_node_annos()
                                        .get_value_for_item(&result_member.node, &anno_key)
                                    {
                                        match by_node.sortable_value(v) {
                                            Ok(sv) => values[i].push(sv),
                                            Err(_) => parse_err_at = Some(by_node.query_index()),
                                        };
                                    }
                                };
                            }
                        }
                        values[0].cmp(&values[1])
                    });
                    if let Some(err_index) = parse_err_at {
                        return Err(anyhow!(
                            "Could not interpret value of node #{err_index} as a number."
                        )
                        .into());
                    }
                }
                let mut offset = 0;
                let mut i_correction = 0;
                let mut visited = BTreeSet::new();
                let mut by_values = vec![String::with_capacity(0); self.by.len()];
                let mut reset_count;
                for (i, mut m) in search_results.into_iter().enumerate() {
                    reset_count = false;
                    let matching_nodes: Result<Vec<String>, GraphAnnisCoreError> = m
                        .iter()
                        .map(|m| {
                            graph
                                .get_node_annos()
                                .get_value_for_item(&m.node, &NODE_NAME_KEY)
                        })
                        .filter_map_ok(|m| m)
                        .map_ok(|m| m.to_string())
                        .collect();
                    let matching_nodes = matching_nodes?;
                    if let Some(target_node) = matching_nodes.get(self.target - 1) {
                        if visited.contains(target_node) {
                            offset += 1;
                        } else {
                            for (bi, ci) in self.by.iter().enumerate() {
                                if let Some(match_entry) = m.get(*ci - 1) {
                                    let coord_anno_key = anno_key_for_match(match_entry);
                                    let internal_id = match_entry.node;
                                    let next_value = graph
                                        .get_node_annos()
                                        .get_value_for_item(&internal_id, &coord_anno_key)?
                                        .unwrap_or_default()
                                        .to_string();
                                    if let Some(previous_value) = by_values.get_mut(bi)
                                        && &next_value != previous_value
                                    {
                                        // reset count
                                        reset_count = true;
                                        previous_value.clear();
                                        previous_value.push_str(&next_value);
                                    }
                                }
                            }
                            if reset_count {
                                i_correction = i;
                                offset = 0;
                            }
                            if let Some(value_i) = self.value {
                                if value_i <= m.len() {
                                    let coord = m.remove(value_i - 1);
                                    let coord_anno_key = anno_key_for_match(&coord);
                                    let internal_id = coord.node;

                                    if let Some(prefix) = graph
                                        .get_node_annos()
                                        .get_value_for_item(&internal_id, &coord_anno_key)?
                                    {
                                        update.add_event(UpdateEvent::AddNodeLabel {
                                            node_name: target_node.to_string(),
                                            anno_ns: self.label.ns.to_string(),
                                            anno_name: self.label.name.to_string(),
                                            anno_value: format!(
                                                "{prefix}-{}",
                                                i + self.start - offset - i_correction
                                            ),
                                        })?;
                                    }
                                }
                            } else {
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: target_node.to_string(),
                                    anno_ns: self.label.ns.to_string(),
                                    anno_name: self.label.name.to_string(),
                                    anno_value: (i + self.start - offset - i_correction)
                                        .to_string(),
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
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

fn anno_key_for_match(entry: &Match) -> Arc<AnnoKey> {
    let anno_key = if entry.anno_key.eq(&NODE_TYPE_KEY) {
        // Replace the generic search key with the token value
        TOKEN_KEY.clone()
    } else {
        entry.anno_key.clone()
    };

    anno_key.clone()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{
        AnnotationGraph,
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS, types::AnnoKey};
    use insta::assert_snapshot;
    use itertools::Itertools;

    use crate::{
        StepID,
        exporter::graphml::GraphMLExporter,
        manipulator::{Manipulator, enumerate::SortByNode},
        test_util::{compare_results, export_to_string},
        util::example_generator,
        util::update_graph_silent,
    };

    use super::EnumerateMatches;

    #[test]
    fn serialize() {
        let module = EnumerateMatches::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = EnumerateMatches {
            queries: vec!["norm _=_ num _=_ pos @* doc".to_string()],
            by: vec![3],
            sort: vec![SortByNode::AsInteger { numeric: 2 }],
            target: 1,
            label: AnnoKey {
                name: "id".into(),
                ns: "stats".into(),
            },
            value: Some(1),
            start: 1,
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
        let module = EnumerateMatches::default();
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
        assert!(graph.global_statistics.is_some());
    }

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

    #[test]
    fn by() {
        let mut update = GraphUpdate::default();
        example_generator::create_corpus_structure_two_documents(&mut update);
        example_generator::create_multiple_segmentations(&mut update, "root/doc1");
        example_generator::create_tokens(&mut update, Some("root/doc2"));
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        assert!(
            EnumerateMatches {
                queries: vec!["tok @* doc".to_string()],
                by: vec![2],
                sort: vec![],
                target: 1,
                ..Default::default()
            }
            .manipulate_corpus(
                &mut graph,
                Path::new("./"),
                StepID {
                    module_name: "test_enumerate".to_string(),
                    path: None
                },
                None
            )
            .is_ok()
        );
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn sort_numeric() {
        let g = base_graph(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(
            EnumerateMatches {
                queries: vec!["sentiment_n".to_string()],
                by: vec![],
                sort: vec![SortByNode::AsInteger { numeric: 1 }],
                target: 1,
                ..Default::default()
            }
            .manipulate_corpus(
                &mut graph,
                Path::new("./"),
                StepID {
                    module_name: "test_enumerate".to_string(),
                    path: None
                },
                None
            )
            .is_ok()
        );
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn sort_numeric_fail() {
        let mut update = GraphUpdate::default();
        example_generator::create_corpus_structure_two_documents(&mut update);
        example_generator::create_multiple_segmentations(&mut update, "root/doc1");
        example_generator::create_tokens(&mut update, Some("root/doc2"));
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let enmr = EnumerateMatches {
            queries: vec!["tok @* doc".to_string()],
            by: vec![],
            sort: vec![SortByNode::AsInteger { numeric: 1 }],
            target: 1,
            ..Default::default()
        }
        .manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_enumerate".to_string(),
                path: None,
            },
            None,
        );

        assert!(enmr.is_err());
        assert_snapshot!(enmr.err().unwrap().to_string());
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
            by: vec![],
            sort: vec![],
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
            by: vec![],
            sort: vec![],
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
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t1".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment_n".to_string(),
            anno_value: "10".to_string(),
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
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t2".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment_n".to_string(),
            anno_value: "0".to_string(),
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
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/document#t3".to_string(),
            anno_ns: "".to_string(),
            anno_name: "sentiment_n".to_string(),
            anno_value: "5".to_string(),
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
