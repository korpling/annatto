use crate::error::{AnnattoError, StandardErrorResult};
use crate::workflow::{StatusMessage, StatusSender};
use crate::{Manipulator, StepID};
use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{Component, Edge},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
    types::{AnnoKey, ComponentType},
    util::split_qname,
};
use itertools::Itertools;
use serde_derive::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::path::Path;
use struct_field_names_as_array::FieldNamesAsSlice;

/// Merge multiple imported corpora into one corpus.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Merge {
    /// Define how to handle merging errrors.
    /// Use "fail" if the whole conversion should fail, "drop" to remove the
    /// documents having errors and "forward" to keep the documents and just
    /// report the errors.
    #[serde(default)]
    error_policy: ErrorPolicy,
    check_names: Vec<String>,
    keep_name: String,
    #[serde(default)]
    optional_values: BTreeSet<String>,
    #[serde(default)]
    optional_chars: BTreeSet<char>,
    #[serde(default)]
    silent: bool,
    #[serde(default)]
    report_details: bool,
    #[serde(default)]
    skip_components: Option<Vec<String>>,
}

#[derive(Default, Deserialize)]
enum ErrorPolicy {
    #[default]
    Fail,
    Drop,
    Forward,
}

impl TryFrom<Option<&String>> for ErrorPolicy {
    type Error = anyhow::Error;
    fn try_from(value: Option<&String>) -> Result<Self, Self::Error> {
        match value {
            None => Ok(ErrorPolicy::default()),
            Some(v) => ErrorPolicy::try_from(v),
        }
    }
}

impl TryFrom<&String> for ErrorPolicy {
    type Error = anyhow::Error;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match &value.trim().to_lowercase()[..] {
            "fail" => Ok(ErrorPolicy::Fail),
            "drop" => Ok(ErrorPolicy::Drop),
            "forward" => Ok(ErrorPolicy::Forward),
            _ => Err(anyhow!("Undefined error policy: {value}")),
        }
    }
}

fn clean_value(value: &str, remove_chars: &BTreeSet<char>) -> String {
    let mut cleaned_value = String::new();
    for c in value.chars() {
        if !remove_chars.contains(&c) {
            cleaned_value.push(c);
        }
    }
    cleaned_value
}

type NodeIdCollection = std::vec::IntoIter<u64>;

impl Merge {
    fn retrieve_ordered_nodes<'a>(
        &self,
        graph: &AnnotationGraph,
        order_names: &'a Vec<String>,
        step_id: &StepID,
    ) -> StandardErrorResult<BTreeMap<String, BTreeMap<&'a str, NodeIdCollection>>> {
        let mut ordered_items_by_doc: BTreeMap<String, BTreeMap<&str, NodeIdCollection>> =
            BTreeMap::new();
        let node_annos = graph.get_node_annos();
        for order_name in order_names {
            let c = AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                order_name.into(),
            );
            if let Some(ordering) = graph.get_graphstorage(&c) {
                let start_nodes = ordering
                    .source_nodes()
                    .filter(|n| ordering.get_ingoing_edges(*n.as_ref().unwrap()).count() == 0)
                    .collect_vec();
                // there should be one start node per document
                let doc_names = start_nodes
                    .iter()
                    .map(|n| {
                        node_annos
                            .get_value_for_item(n.as_ref().unwrap(), &NODE_NAME_KEY)
                            .unwrap()
                            .unwrap()
                    })
                    .map(|v| {
                        v.split('/')
                            .last()
                            .unwrap()
                            .split('#')
                            .next()
                            .unwrap()
                            .to_string()
                    })
                    .collect_vec();
                for doc_name in &doc_names {
                    if !ordered_items_by_doc.contains_key(doc_name) {
                        ordered_items_by_doc.insert(doc_name.to_string(), BTreeMap::new());
                    }
                }
                for (start_node_r, doc_name) in start_nodes.into_iter().zip(doc_names) {
                    let start_node = start_node_r?;
                    let mut nodes: Vec<u64> = Vec::new();
                    nodes.push(start_node);
                    let dfs =
                        CycleSafeDFS::new(ordering.as_edgecontainer(), start_node, 1, usize::MAX);
                    for entry in dfs {
                        nodes.push(entry?.node);
                    }
                    if nodes.is_empty() {
                        let err = AnnattoError::Manipulator {
                            reason: format!("Ordering `{order_name}` does not connect any nodes."),
                            manipulator: step_id.module_name.to_string(),
                        };
                        return Err(Box::new(err));
                    }
                    ordered_items_by_doc
                        .get_mut(&doc_name)
                        .unwrap()
                        .insert(order_name, nodes.into_iter());
                }
            } else {
                let err = AnnattoError::Manipulator {
                    reason: format!("Required ordering `{order_name}` does not exist."),
                    manipulator: step_id.module_name.to_string(),
                };
                return Err(Box::new(err));
            }
        }
        Ok(ordered_items_by_doc)
    }

    fn merge_all_components(
        &self,
        graph: &AnnotationGraph,
        updates: &mut GraphUpdate,
        skip_components: BTreeSet<AnnotationComponent>,
        node_map: BTreeMap<u64, u64>,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (edge_component_type, switch_source) in [
            (AnnotationComponentType::Coverage, false),
            (AnnotationComponentType::Dominance, false),
            (AnnotationComponentType::Pointing, true),
        ] {
            for edge_component in graph.get_all_components(Some(edge_component_type.clone()), None)
            {
                if skip_components.contains(&edge_component) {
                    if let Some(sender) = tx {
                        let message = format!("Skipping component {}", &edge_component.name);
                        sender.send(StatusMessage::Info(message))?;
                    }
                    continue;
                }
                self.merge_component(graph, updates, edge_component, switch_source, &node_map, tx)?;
            }
        }
        Ok(())
    }

    fn merge_component(
        &self,
        graph: &AnnotationGraph,
        updates: &mut GraphUpdate,
        edge_component: Component<AnnotationComponentType>,
        switch_source: bool,
        node_map: &BTreeMap<u64, u64>,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut report_missing = BTreeSet::new();
        let edge_component_name = &edge_component.name;
        let edge_component_type = edge_component.get_type();
        let layer_name = edge_component.layer.to_string();
        let node_annos = graph.get_node_annos();
        if let Some(edge_storage) = graph.get_graphstorage(&edge_component) {
            // there are some coverage edges
            let edge_annos = edge_storage.get_anno_storage();
            for source_node_r in edge_storage.source_nodes() {
                let source_node = source_node_r?;
                let source_node_name = node_annos
                    .get_value_for_item(&source_node, &NODE_NAME_KEY)?
                    .unwrap(); // existence guaranteed
                let new_source_name = if switch_source {
                    if let Some(new_source_node) = node_map.get(&source_node) {
                        node_annos
                            .get_value_for_item(new_source_node, &NODE_NAME_KEY)?
                            .unwrap()
                            .to_string()
                    } else {
                        if let Some(sender) = tx {
                            let message = format!("Could not determine new source of an edge in component {}/{}, the edge will be dropped", &edge_component_type.to_string(), &edge_component_name);
                            sender.send(StatusMessage::Warning(message))?;
                        }
                        continue;
                    }
                } else {
                    source_node_name.to_string()
                };
                let edge_dfs =
                    CycleSafeDFS::new(edge_storage.as_edgecontainer(), source_node, 1, 1);
                report_missing.clear();
                for target_r in edge_dfs {
                    let target = target_r?.node;
                    if let Some(new_target) = node_map.get(&target) {
                        // new child still exists in target graph
                        let target_name = node_annos
                            .get_value_for_item(&target, &NODE_NAME_KEY)?
                            .unwrap(); // existence guaranteed
                        let new_target_name = node_annos
                            .get_value_for_item(new_target, &NODE_NAME_KEY)?
                            .unwrap();
                        updates.add_event(UpdateEvent::DeleteEdge {
                            source_node: source_node_name.to_string(),
                            target_node: target_name.to_string(),
                            layer: layer_name.to_string(),
                            component_type: edge_component_type.to_string(),
                            component_name: edge_component_name.to_string(),
                        })?;
                        updates.add_event(UpdateEvent::AddEdge {
                            source_node: new_source_name.clone(),
                            target_node: new_target_name.to_string(),
                            layer: layer_name.to_string(),
                            component_type: edge_component_type.to_string(),
                            component_name: edge_component_name.to_string(),
                        })?;
                        // check edge for annotations that need to be transferred
                        let edge = Edge {
                            source: source_node,
                            target,
                        }; // TODO at least for the case of pointing relations the same container might contain more than one edge, or am I wrong?
                        for k in edge_annos.get_all_keys_for_item(&edge, None, None)? {
                            if k.ns != ANNIS_NS {
                                let v = edge_annos.get_value_for_item(&edge, &k)?.unwrap(); // guaranteed to exist
                                let u = UpdateEvent::AddEdgeLabel {
                                    source_node: new_source_name.clone(),
                                    target_node: new_target_name.to_string(),
                                    layer: layer_name.to_string(),
                                    component_type: edge_component_type.to_string(),
                                    component_name: edge_component_name.to_string(),
                                    anno_ns: k.ns.to_string(),
                                    anno_name: k.name.to_string(),
                                    anno_value: v.to_string(),
                                };
                                updates.add_event(u)?;
                            }
                        }
                    } else {
                        // child does not exist in target graph, which must be legal if we allow texts to only partially match (e. g. in the case of dropped punctuation)
                        // it could also be the case that the source and target node are not in the node_map bc they are not ordered nodes and thus don't require to be modified
                        report_missing.insert(
                            node_annos
                                .get_value_for_item(&target, &NODE_NAME_KEY)?
                                .unwrap(),
                        );
                    }
                }
                if !report_missing.is_empty() && tx.is_some() {
                    let sender = tx.as_ref().unwrap();
                    sender.send(StatusMessage::Info(format!(
                        "Not all children of node {} ({}::{}) available in target graph: {:?}",
                        &source_node_name, &layer_name, &edge_component_name, &report_missing
                    )))?;
                }
            }
        }
        Ok(())
    }

    fn handle_document_errors(
        &self,
        graph: &AnnotationGraph,
        updates: &mut GraphUpdate,
        docs_with_errors: BTreeSet<String>,
        policy: &ErrorPolicy,
        step_id: &StepID,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let node_annos = graph.get_node_annos();
        let n = docs_with_errors.len();
        let mut collected_errors = Vec::new();
        if n > 0 {
            let docs_s = docs_with_errors.iter().sorted().join("\n");

            match policy {
                ErrorPolicy::Fail => {
                    let msg = format!("{n} documents with ill-merged tokens:\n{docs_s}");
                    let err = AnnattoError::Manipulator {
                        reason: msg,
                        manipulator: step_id.module_name.to_string(),
                    };
                    collected_errors.push(err);
                }
                ErrorPolicy::Drop => {
                    for doc_node_name in docs_with_errors {
                        // get all doc nodes with doc_id
                        let corpus_nodes = node_annos
                            .exact_anno_search(
                                Some(NODE_TYPE_KEY.ns.as_str()),
                                NODE_TYPE_KEY.name.as_str(),
                                ValueSearch::Some("corpus"),
                            )
                            .map(|m| m.unwrap().node)
                            .collect::<BTreeSet<u64>>();
                        let nodes_with_doc_name = node_annos
                            .exact_anno_search(
                                Some(NODE_NAME_KEY.ns.as_str()),
                                NODE_NAME_KEY.name.as_str(),
                                ValueSearch::Some(doc_node_name.as_str()),
                            )
                            .map(|m| m.unwrap().node)
                            .collect::<BTreeSet<u64>>();
                        for doc_node_id in corpus_nodes.intersection(&nodes_with_doc_name) {
                            let doc_name = node_annos
                                .get_value_for_item(doc_node_id, &NODE_NAME_KEY)?
                                .unwrap();
                            updates.add_event(UpdateEvent::DeleteNode {
                                node_name: doc_name.to_string(),
                            })?;
                        }
                    }

                    if let Some(tx) = tx {
                        let msg = format!("{n} documents with ill-merged tokens will be dropped from the corpus:\n{docs_s}");
                        tx.send(StatusMessage::Warning(msg))?;
                    }
                }
                _ => {
                    if let Some(tx) = tx {
                        let msg = format!("BE AWARE that the corpus contains severe merging issues in the following {n} documents:\n{docs_s}");
                        tx.send(StatusMessage::Warning(msg))?;
                    }
                }
            };
        }

        if collected_errors.is_empty() {
            Ok(())
        } else {
            Err(AnnattoError::ConversionFailed {
                errors: collected_errors,
            }
            .into())
        }
    }

    fn collect_skip_components(
        &self,
        graph: &AnnotationGraph,
    ) -> BTreeSet<Component<AnnotationComponentType>> {
        let autogenerated_components =
            AnnotationComponentType::update_graph_index_components(graph)
                .into_iter()
                .collect::<BTreeSet<AnnotationComponent>>();
        let mut skip_components = autogenerated_components;
        if let Some(ref skip_component_spec) = self.skip_components {
            for spec in skip_component_spec {
                let split_spec = split_qname(spec);
                let layer = match split_spec.0 {
                    None => "",
                    Some(v) => v,
                };
                let name = split_spec.1;
                for c in graph.get_all_components(None, Some(name)) {
                    if c.layer == layer {
                        skip_components.insert(AnnotationComponent::new(
                            c.get_type(),
                            layer.into(),
                            name.into(),
                        ));
                    }
                }
            }
        }
        skip_components
    }
}

struct TextNodeMapper {
    module_name: String,
    docs_with_errors: BTreeSet<String>,
    target_key: AnnoKey,
    optional_chars: BTreeSet<char>,
    optionals: BTreeSet<String>,
}

impl TextNodeMapper {
    fn map_text_nodes(
        &mut self,
        graph: &AnnotationGraph,
        updates: &mut GraphUpdate,
        ordered_items_by_doc: BTreeMap<String, BTreeMap<&str, NodeIdCollection>>,
        tx: &Option<StatusSender>,
    ) -> Result<BTreeMap<u64, u64>, Box<dyn std::error::Error>> {
        let mut node_map: BTreeMap<u64, u64> = BTreeMap::new();
        let node_annos = graph.get_node_annos();
        for (doc_name, mut ordered_items_by_name) in ordered_items_by_doc {
            let ordered_keep_items_opt =
                ordered_items_by_name.remove(self.target_key.name.as_str());
            if ordered_keep_items_opt.is_none() {
                if let Some(sender) = tx {
                    let message = format!(
                        "Document {} does not contain an ordering {}",
                        &doc_name, &self.target_key.name
                    );
                    sender.send(StatusMessage::Warning(message))?;
                }
                self.docs_with_errors.insert(doc_name);
                continue;
            }
            let ordered_keep_items = ordered_keep_items_opt.unwrap();
            let mut order_names = BTreeSet::new();
            for k in ordered_items_by_name.keys() {
                order_names.insert(k.to_string());
            }
            let mut unused_by_name = BTreeMap::new();
            for item in ordered_keep_items {
                let ref_val = match node_annos.get_value_for_item(&item, &self.target_key)? {
                    Some(v) => v,
                    None => {
                        let critical_node = node_annos
                            .get_value_for_item(&item, &NODE_NAME_KEY)?
                            .unwrap();
                        return Err(Box::new(AnnattoError::Manipulator {
                            reason: format!(
                                "Could not determine annotation value for key {}::{} @ {}",
                                self.target_key.ns, self.target_key.name, critical_node
                            ),
                            manipulator: self.module_name.clone(),
                        }));
                    }
                };
                let ref_is_optional = self.optionals.contains(&ref_val.to_string());
                let ref_node_name = node_annos
                    .get_value_for_item(&item, &NODE_NAME_KEY)?
                    .unwrap(); // existence guaranteed
                for other_name in &order_names {
                    let mut finished = false;
                    while !finished {
                        let other_opt = if unused_by_name.contains_key(other_name) {
                            unused_by_name.remove(other_name)
                        } else {
                            ordered_items_by_name
                                .get_mut(other_name.as_str())
                                .unwrap()
                                .next()
                        };
                        if let Some(other_item) = other_opt {
                            let other_key = AnnoKey {
                                ns: "".into(),
                                name: other_name.into(),
                            };
                            let other_val = node_annos
                                .get_value_for_item(&other_item, &other_key)?
                                .unwrap();
                            if ref_val == other_val
                                || !self.optional_chars.is_empty()
                                    && clean_value(&ref_val, &self.optional_chars)
                                        == clean_value(&other_val, &self.optional_chars)
                            {
                                // text values match
                                let anno_keys =
                                    node_annos.get_all_keys_for_item(&other_item, None, None)?;
                                // annotations directly on the ordered node
                                for ak in anno_keys {
                                    let anno_name = ak.name.to_string();
                                    if ak.ns != ANNIS_NS && !order_names.contains(&anno_name) {
                                        let av = node_annos
                                            .get_value_for_item(&other_item, ak.as_ref())?
                                            .unwrap(); // existence guaranteed
                                        updates.add_event(UpdateEvent::AddNodeLabel {
                                            node_name: ref_node_name.to_string(),
                                            anno_ns: ak.ns.to_string(),
                                            anno_name,
                                            anno_value: av.to_string(),
                                        })?;
                                    }
                                }
                                // delete ordered node, the rest (edges and labels) should theoretically die as a consequence
                                let other_node_name = node_annos
                                    .get_value_for_item(&other_item, &NODE_NAME_KEY)?
                                    .unwrap()
                                    .to_string(); // existence guaranteed
                                updates.add_event(UpdateEvent::DeleteNode {
                                    node_name: other_node_name,
                                })?;
                                node_map.insert(other_item, item);
                                finished = true;
                            } else {
                                // text values don't match
                                let other_is_optional =
                                    self.optionals.contains(&other_val.to_string());
                                if ref_is_optional && !other_is_optional {
                                    // advance outer, do not advance inner
                                    unused_by_name.insert(other_name.to_string(), other_item);
                                    finished = true;
                                } else if !ref_is_optional && other_is_optional {
                                    // advance inner, do not advance outer
                                    // i. e. do nothing
                                } else if !ref_is_optional && !other_is_optional {
                                    // match expected, advance both
                                    if let Some(sender) = tx {
                                        let message = StatusMessage::Warning(format!("{}: {}={} and {}={} do not match. Mismatch could not be resolved.", &doc_name, &self.target_key.name, ref_val, other_name, other_val));
                                        sender.send(message)?;
                                    }
                                    node_map.insert(other_item, item); // map anyway to be compliant with ErrorPolicy::Forward
                                    self.docs_with_errors.insert(doc_name.to_string());
                                    finished = true;
                                } else {
                                    // both optional, but non-matching, advance both
                                    finished = true;
                                }
                            }
                        } else {
                            // no further nodes
                            if !ref_is_optional {
                                let message = format!(
                                    "Ran out of nodes for ordering `{}` in document {}",
                                    other_name, &doc_name
                                );
                                if let Some(sender) = tx {
                                    sender.send(StatusMessage::Warning(message))?;
                                    self.docs_with_errors.insert(doc_name.to_string());
                                }
                            }
                            finished = true;
                        }
                    }
                }
            }
        }
        Ok(node_map)
    }
}

impl Manipulator for Merge {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        _workflow_directory: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &tx {
            sender.send(StatusMessage::Info(String::from("Starting merge")))?;
        }
        // read properties
        let on_error = &self.error_policy;
        let order_names = &self.check_names;
        let keep_name = self.keep_name.to_string();
        let keep_name_key = AnnoKey {
            ns: "".into(),
            name: keep_name.into(),
        };
        let optional_toks = self.optional_values.clone();
        let optional_chars = self.optional_chars.clone();
        let silent = self.silent;
        let report_details = self.report_details;
        let sender_opt = if silent { None } else { tx };
        // init
        let mut updates = GraphUpdate::default();
        // merge
        let ordered_items_by_doc = self.retrieve_ordered_nodes(graph, order_names, &step_id)?;
        let reporter = if report_details { &sender_opt } else { &None };
        let mut mapper = TextNodeMapper {
            target_key: keep_name_key,
            module_name: step_id.module_name.to_string(),
            docs_with_errors: BTreeSet::default(),
            optional_chars,
            optionals: optional_toks,
        };
        let node_map: BTreeMap<u64, u64> =
            mapper.map_text_nodes(graph, &mut updates, ordered_items_by_doc, reporter)?;
        let skip_components = self.collect_skip_components(graph);
        self.merge_all_components(graph, &mut updates, skip_components, node_map, &sender_opt)?;
        self.handle_document_errors(
            graph,
            &mut updates,
            mapper.docs_with_errors,
            on_error,
            &step_id,
            &sender_opt,
        )?;
        graph.apply_update(&mut updates, |_msg| {})?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::env::temp_dir;

    use crate::manipulator::merge::{ErrorPolicy, Merge};
    use crate::manipulator::Manipulator;
    use crate::{Result, StepID};

    use graphannis::corpusstorage::{QueryLanguage, ResultOrder, SearchQuery};
    use graphannis::model::AnnotationComponentType;
    use graphannis::update::{GraphUpdate, UpdateEvent};
    use graphannis::{AnnotationGraph, CorpusStorage};
    use graphannis_core::annostorage::ValueSearch;
    use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY};
    use itertools::Itertools;
    use tempfile::{tempdir_in, tempfile};

    #[test]
    fn test_merger_in_mem() {
        let r = core_test(false);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_merger_on_disk() {
        let r = core_test(true);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    fn core_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph(on_disk)?;
        let merger = Merge {
            check_names: vec![
                "norm".to_string(),
                "text".to_string(),
                "syntext".to_string(),
            ],
            error_policy: ErrorPolicy::default(),
            keep_name: "norm".to_string(),
            optional_chars: BTreeSet::new(),
            optional_values: vec!["NOISE".to_string()]
                .into_iter()
                .collect::<BTreeSet<String>>(),
            report_details: false,
            silent: false,
            skip_components: None,
        };
        let step_id = StepID {
            module_name: "merger".to_string(),
            path: None,
        };
        let merge_r = merger.manipulate_corpus(&mut g, temp_dir().as_path(), step_id, None);
        assert_eq!(merge_r.is_ok(), true, "Probing merge result {:?}", &merge_r);
        let mut e_g = expected_output_graph(on_disk)?;
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                e_g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let g_corpus_nodes: BTreeSet<String> = g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes); //TODO clarify: Delegate or assertion?
                                                    // test by components
        let e_c_list = e_g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
            .collect_vec();
        let g_c_list = g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0) // graph might contain empty components after merge
            .collect_vec();
        assert_eq!(
            e_c_list.len(),
            g_c_list.len(),
            "components expected:\n{:?};\ncomponents are:\n{:?}",
            &e_c_list,
            &g_c_list
        );
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o = candidates.get(0);
            assert_eq!(&c, c_o.unwrap());
        }
        // test with queries
        let queries = [
            "norm",
            "dipl",
            "norm _o_ dipl",
            "node >* norm",
            "cat",
            "node ->dep node",
            "node ->dep[deprel=/.+/] node",
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir_in(temp_dir())?;
        let tmp_dir_g = tempdir_in(temp_dir())?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    #[test]
    fn test_export_mem() {
        let export = export_test(false);
        assert_eq!(
            export.is_ok(),
            true,
            "Export test ends with Err: {:?}",
            &export
        );
    }

    #[test]
    fn test_export_disk() {
        let export = export_test(true);
        assert_eq!(
            export.is_ok(),
            true,
            "Export test ends with Err: {:?}",
            &export
        );
    }

    fn export_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph(on_disk)?;
        let merger = Merge {
            check_names: vec![
                "norm".to_string(),
                "text".to_string(),
                "syntext".to_string(),
            ],
            error_policy: ErrorPolicy::Drop,
            keep_name: "norm".to_string(),
            optional_chars: BTreeSet::new(),
            optional_values: BTreeSet::new(),
            report_details: false,
            silent: false,
            skip_components: None,
        };
        let step_id = StepID {
            module_name: "merger".to_string(),
            path: None,
        };
        assert_eq!(
            merger
                .manipulate_corpus(&mut g, temp_dir().as_path(), step_id, None)
                .is_ok(),
            true
        );
        let tmp_file = tempfile()?;
        let export =
            graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }

    fn input_graph(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        // import 1
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a/doc".to_string(),
            target_node: "root/a".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for i in 0..5 {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("root/a/doc#t{}", i),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("root/a/doc#t{}", i),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#t{}", i - 1),
                    target_node: format!("root/a/doc#t{}", i),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode {
            node_name: sentence_span_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: sentence_span_name.to_string(),
            anno_ns: "dipl".to_string(),
            anno_name: "sentence".to_string(),
            anno_value: "1".to_string(),
        })?;
        for (ii, (txt, start, end)) in [("I'm", 0, 2), ("in", 2, 3), ("New", 3, 4), ("York", 4, 5)]
            .iter()
            .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "dipl".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: sentence_span_name.to_string(),
                target_node: name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "dipl".to_string(),
                })?;
            }
        }
        // fake-tok 2
        for (ii, (txt, start, end, pos_label)) in [
            ("I", 0, 1, "PRON"),
            ("am", 1, 2, "VERB"),
            ("in", 2, 3, "ADP"),
            ("New York", 3, 5, "PROPN"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "norm".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "norm".to_string(),
                anno_name: "pos".to_string(),
                anno_value: pos_label.to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if ii > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "norm".to_string(),
                })?;
            }
        }
        // import 2
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b/doc".to_string(),
            target_node: "root/b".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for (ii, (txt, lemma_label)) in [
            ("I", "I"),
            ("NOISE", "NOISE"),
            ("am", "be"),
            ("in", "in"),
            ("New York", "New York"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/b/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "text".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma_label.to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/b/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "text".to_string(),
                })?;
            }
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        let deprel_name = "deprel";
        for (source, target, label) in
            [(3, 1, "subj"), (3, 4, "comp:pred"), (4, 5, "comp:obj")].iter()
        {
            let source_name = format!("root/b/doc#t{}", source);
            let target_name = format!("root/b/doc#t{}", target);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_name,
                target_node: target_name,
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
                anno_ns: "".to_string(),
                anno_name: deprel_name.to_string(),
                anno_value: label.to_string(),
            })?;
        }
        // import 3
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/c".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/c".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/c/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/c/doc".to_string(),
            target_node: "root/c".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for (ii, (txt, lemma_label)) in [
            ("I", "I"),
            ("am", "be"),
            ("in", "in"),
            ("New York", "New York"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/c/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "syntext".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma_label.to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/c/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "syntext".to_string(),
                })?;
            }
        }
        let pp_struct_name = "root/c/doc#structpp";
        let sbj_struct_name = "root/c/doc#structsbj";
        let vp_struct_name = "root/c/doc#structvp";
        let cp_struct_name = "root/c/doc#structcp";
        let anno_name = "cat";
        let syn_anno_layer_name = "syntax";
        let syn_component_name = "constituents";
        for (node_id, label, targets) in [
            (sbj_struct_name, "sbj", ["root/c/doc#t1"].to_vec()),
            (
                pp_struct_name,
                "pp",
                ["root/c/doc#t3", "root/c/doc#t4"].to_vec(),
            ),
            (
                vp_struct_name,
                "vp",
                ["root/c/doc#t2", pp_struct_name].to_vec(),
            ),
            (
                cp_struct_name,
                "cp",
                [sbj_struct_name, vp_struct_name].to_vec(),
            ),
        ]
        .iter()
        {
            u.add_event(UpdateEvent::AddNode {
                node_name: node_id.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_id.to_string(),
                anno_ns: "".to_string(),
                anno_name: anno_name.to_string(),
                anno_value: label.to_string(),
            })?;
            for target in targets.into_iter() {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_id.to_string(),
                    target_node: target.to_string(),
                    layer: syn_anno_layer_name.to_string(),
                    component_type: AnnotationComponentType::Dominance.to_string(),
                    component_name: syn_component_name.to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn expected_output_graph(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        // import 1
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a/doc".to_string(),
            target_node: "root/a".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for i in 0..5 {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("root/a/doc#t{}", i),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("root/a/doc#t{}", i),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#t{}", i - 1),
                    target_node: format!("root/a/doc#t{}", i),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode {
            node_name: sentence_span_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: sentence_span_name.to_string(),
            anno_ns: "dipl".to_string(),
            anno_name: "sentence".to_string(),
            anno_value: "1".to_string(),
        })?;
        for (ii, (txt, start, end)) in [("I'm", 0, 2), ("in", 2, 3), ("New", 3, 4), ("York", 4, 5)]
            .iter()
            .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "dipl".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: sentence_span_name.to_string(),
                target_node: name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "dipl".to_string(),
                })?;
            }
        }
        // fake-tok 2
        for (ii, (txt, start, end, pos_label)) in [
            ("I", 0, 1, "PRON"),
            ("am", 1, 2, "VERB"),
            ("in", 2, 3, "ADP"),
            ("New York", 3, 5, "PROPN"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "norm".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "norm".to_string(),
                anno_name: "pos".to_string(),
                anno_value: pos_label.to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if ii > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "norm".to_string(),
                })?;
            }
        }
        // import 2 (after merge)
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b/doc".to_string(),
            target_node: "root/b".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for (ii, lemma_label) in ["I", "be", "in", "New York"].iter().enumerate() {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma_label.to_string(),
            })?;
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        let deprel_name = "deprel";
        for (source, target, label) in
            [(6, 5, "subj"), (6, 7, "comp:pred"), (7, 8, "comp:obj")].iter()
        {
            let source_name = format!("root/a/doc#s{}", source);
            let target_name = format!("root/a/doc#s{}", target);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_name,
                target_node: target_name,
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
                anno_ns: "".to_string(),
                anno_name: deprel_name.to_string(),
                anno_value: label.to_string(),
            })?;
        }
        // import 3 (after merge)
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/c".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/c".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/c/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/c/doc".to_string(),
            target_node: "root/c".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let pp_struct_name = "root/c/doc#structpp";
        let sbj_struct_name = "root/c/doc#structsbj";
        let vp_struct_name = "root/c/doc#structvp";
        let cp_struct_name = "root/c/doc#structcp";
        let anno_name = "cat";
        let syn_anno_layer_name = "syntax";
        let syn_component_name = "constituents";
        for (node_id, label, targets) in [
            (sbj_struct_name, "sbj", ["root/a/doc#s1"].to_vec()),
            (
                pp_struct_name,
                "pp",
                ["root/a/doc#s3", "root/a/doc#s4"].to_vec(),
            ),
            (
                vp_struct_name,
                "vp",
                ["root/a/doc#s2", pp_struct_name].to_vec(),
            ),
            (
                cp_struct_name,
                "cp",
                [sbj_struct_name, vp_struct_name].to_vec(),
            ),
        ]
        .iter()
        {
            u.add_event(UpdateEvent::AddNode {
                node_name: node_id.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_id.to_string(),
                anno_ns: "".to_string(),
                anno_name: anno_name.to_string(),
                anno_value: label.to_string(),
            })?;
            for target in targets.into_iter() {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_id.to_string(),
                    target_node: target.to_string(),
                    layer: syn_anno_layer_name.to_string(),
                    component_type: AnnotationComponentType::Dominance.to_string(),
                    component_name: syn_component_name.to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }
}
