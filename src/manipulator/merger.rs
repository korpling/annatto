use std::collections::{HashMap,HashSet,BTreeMap,Bound};
use std::convert::TryFrom;
use std::fmt::format;
use std::hash::Hash;
use std::iter::FromIterator;
use crate::{Manipulator,Module};
use crate::error::AnnattoError;
use crate::workflow::{StatusMessage,StatusSender};
use crate::util::write_to_file;
use graphannis::{
    graph::{Component,Edge},
    model::{AnnotationComponentType,AnnotationComponent},
    update,
    update::{GraphUpdate,UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::{ValueSearch,Match},
    dfs::CycleSafeDFS,
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS,NODE_NAME_KEY},
    types::{AnnoKey,ComponentType},
    util::{join_qname,split_qname}
};
use itertools::Itertools;
use smartstring;
use itertools::Zip;

pub struct Merger {}


impl Default for Merger {
    fn default() -> Self {
        Merger {}
    }
}

const PROP_CHECK_NAMES: &str = "check.names";
const PROP_KEEP_NAME: &str = "keep.name";
const PROP_ON_ERROR: &str = "on.error";
const PROP_SKIP_COMPONENTS: &str = "skip.components";
const PROPVAL_SEP: &str = ",";


enum OnErrorValues {    
    Fail,
    Drop,
    Forward
}

impl TryFrom<Option<&String>> for OnErrorValues {
    type Error = AnnattoError;
    fn try_from(value: Option<&String>) -> Result<Self, Self::Error> {
        match value {
            None => Ok(OnErrorValues::default()),
            Some(v) => OnErrorValues::try_from(v)
        }
    }
}

impl TryFrom<&String> for OnErrorValues {
    type Error = AnnattoError;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match &value.trim().to_lowercase()[..] {
            "fail" => Ok(OnErrorValues::Fail),
            "drop" => Ok(OnErrorValues::Drop),
            "forward" => Ok(OnErrorValues::Forward),
            _ => Err(AnnattoError::Manipulator { 
                reason: format!("Undefined value for property {}: {}", PROP_ON_ERROR, value), 
                manipulator: String::from(MODULE_NAME) })
        }
    }
}

impl Default for OnErrorValues {
    fn default() -> Self {
        OnErrorValues::Fail
    }
}

impl Manipulator for Merger {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &tx {
            sender.send(StatusMessage::Info(String::from("Starting merge")))?;
        }
        let on_error = OnErrorValues::try_from(properties.get(PROP_ON_ERROR))?;
        let order_names = properties.get(PROP_CHECK_NAMES).unwrap().split(PROPVAL_SEP).collect::<Vec<&str>>();    
        let node_annos = graph.get_node_annos();        
        let keep_name = properties.get(PROP_KEEP_NAME).unwrap();
        let keep_name_key = AnnoKey { ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(keep_name) };
        let skip_component_spec_o = properties.get(PROP_SKIP_COMPONENTS);
        let mut updates = GraphUpdate::default();
        // gather ordered tokens
        let mut docs_with_errors = HashSet::new();
        let mut ordered_items_by_doc: HashMap<String, HashMap<&str, std::vec::IntoIter<u64>>> = HashMap::new();
        for o_n in order_names.clone() {
            let order_name = o_n;
            let c = AnnotationComponent::new(AnnotationComponentType::Ordering, smartstring::alias::String::from(ANNIS_NS), smartstring::alias::String::from(order_name));            
            if let Some(ordering) = graph.get_graphstorage(&c) {                
                let start_nodes = ordering.source_nodes().filter(|n| ordering.get_ingoing_edges(*n.as_ref().unwrap()).count() == 0).collect_vec();
                // there should be one start node per document
                let doc_names = start_nodes.iter()
                                            .map(|n| node_annos.get_value_for_item(n.as_ref().unwrap(), &NODE_NAME_KEY).unwrap().unwrap())
                                            .map(|v| v.split("/").last().unwrap().split("#").next().unwrap().to_string())
                                            .collect_vec();
                for doc_name in &doc_names {
                    if !ordered_items_by_doc.contains_key(doc_name) {
                        ordered_items_by_doc.insert(doc_name.to_string(), HashMap::new());
                    }
                }
                for (start_node_r, doc_name) in start_nodes.into_iter().zip(doc_names) {
                    let start_node = start_node_r?;
                    let mut nodes: Vec<u64> = Vec::new();
                    nodes.push(start_node);
                    let dfs = CycleSafeDFS::new(ordering.as_edgecontainer(), start_node, 1, usize::MAX);
                    for entry in dfs {
                        nodes.push(entry?.node);
                    }
                    if nodes.is_empty() {                 
                        panic!("No nodes for ordering `{}` could be retrieved.", order_name);
                    }
                    ordered_items_by_doc.get_mut(&doc_name).unwrap().insert(order_name, nodes.into_iter());
                }
            } else {
                panic!("No ordering with name {}", order_name); //TODO
            }            
        }
        // set up some trackers
        let mut node_map: HashMap<u64, u64> = HashMap::new();  // maps node ids of old annotations owners to new (remaining) annotation owners, relevant for mapping edges later
        // merge or align
        for (doc_name, mut ordered_items_by_name) in ordered_items_by_doc {
            let mut ordered_keep_items = ordered_items_by_name.remove(keep_name.as_str()).unwrap();               
            for item in ordered_keep_items {
                let ref_val = node_annos.get_value_for_item(&item, &keep_name_key)?.unwrap();  // by definition this has to exist
                let ref_node_name = node_annos.get_value_for_item(&item, &NODE_NAME_KEY)?.unwrap();  // existence guaranteed
                for name in &order_names {
                    let other_name = *name;
                    if other_name == keep_name {
                        continue;
                    }
                    if let Some(other_item) = ordered_items_by_name.get_mut(other_name).unwrap().next() {
                        let other_key = AnnoKey {ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(other_name)};
                        let other_val = node_annos.get_value_for_item(&other_item, &other_key)?.unwrap();
                        if ref_val == other_val {  // text values match
                            // align or merge
                            // case merge
                            let anno_keys = node_annos.get_all_keys_for_item(&other_item, None, None)?;
                            // annotations directly on the ordered node
                            for ak in anno_keys {
                                if ak.ns != ANNIS_NS && !order_names.contains(&ak.name.as_str()) {                                
                                    let av = node_annos.get_value_for_item(&other_item, ak.as_ref())?.unwrap();  // existence guaranteed
                                    updates.add_event(UpdateEvent::AddNodeLabel { node_name: ref_node_name.to_string(),
                                                                                anno_ns: ak.ns.to_string(), 
                                                                                anno_name: ak.name.to_string(), 
                                                                                anno_value: av.to_string() })?;
                                }
                            }
                            // delete ordered node, the rest (edges and labels) should theoretically die as a consequence
                            let other_node_name = node_annos.get_value_for_item(&other_item, &NODE_NAME_KEY)?.unwrap().to_string();  // existence guaranteed
                            updates.add_event(UpdateEvent::DeleteNode { node_name: other_node_name })?;                        
                            node_map.insert(other_item, item);                        
                        } else {  // text values don't match
                            // alternative 
                            // TODO implement logic for punctuation etc, for now just fail                            
                            docs_with_errors.insert(doc_name.to_string());
                        }                    
                    } else {
                        // no further nodes
                        panic!("No item available for {}", other_name);
                    }
                }
            }
        }
        // do edges
        let autogenerated_components = AnnotationComponentType::update_graph_index_components(graph).into_iter().collect::<HashSet<AnnotationComponent>>();
        let mut skip_components = HashSet::from(autogenerated_components);
        if let Some(skip_component_spec) = skip_component_spec_o {
            for spec in skip_component_spec.split(PROPVAL_SEP) {
                let split_spec = split_qname(spec);
                let layer = match split_spec.0 {
                    None => smartstring::alias::String::from(""),
                    Some(v) => smartstring::alias::String::from(v)
                };
                let name = smartstring::alias::String::from(split_spec.1);
                for c in graph.get_all_components(None, Some(name.as_str())) {
                    if c.layer == layer {
                        skip_components.insert(AnnotationComponent::new(c.get_type(), layer.clone(), name.clone()));
                    }
                }
            }
        }
        let mut report_missing = HashSet::new();
        for (edge_component_type, switch_source) in [(AnnotationComponentType::Coverage, false), 
                                                             (AnnotationComponentType::Dominance, false), 
                                                             (AnnotationComponentType::Pointing, true)] {
            for edge_component in graph.get_all_components(Some(edge_component_type.clone()), None) {
                if skip_components.contains(&edge_component) {
                    if let Some(sender) = &tx {
                        let message = format!("Skipping component {}", &edge_component.name);
                        sender.send(StatusMessage::Info(message))?;
                    }
                    continue;
                }                
                let edge_component_name = &edge_component.name;                    
                let layer_name = edge_component.layer.to_string();
                if let Some(edge_storage) = graph.get_graphstorage(&edge_component) {
                    // there are some coverage edges
                    let edge_annos = edge_storage.get_anno_storage();
                    for source_node_r in edge_storage.source_nodes() {
                        let source_node = source_node_r?;
                        let source_node_name = node_annos.get_value_for_item(&source_node, &NODE_NAME_KEY)?.unwrap(); // existence guaranteed
                        let new_source_name = if switch_source {
                            if let Some(new_source_node) = node_map.get(&source_node) {
                                node_annos.get_value_for_item(new_source_node, &NODE_NAME_KEY)?.unwrap().to_string()
                            } else {
                                if let Some(sender) = &tx {
                                    let message = format!("Could not determine new source of an edge in component {}/{}, the edge will be dropped", &edge_component_type.to_string(), &edge_component_name);
                                    sender.send(StatusMessage::Warning(message));                                    
                                }
                                continue;
                            }
                        } else {
                            source_node_name.to_string()
                        };
                        let edge_dfs = CycleSafeDFS::new(edge_storage.as_edgecontainer(), source_node, 1, 1);
                        report_missing.clear();
                        for target_r in edge_dfs {
                            let target = target_r?.node;       
                            if let Some(new_target) = node_map.get(&target) {                            
                                // new child still exists in target graph
                                let target_name = node_annos.get_value_for_item(&target, &NODE_NAME_KEY)?.unwrap();  // existence guaranteed    
                                let new_target_name = node_annos.get_value_for_item(new_target, &NODE_NAME_KEY)?.unwrap();                                
                                updates.add_event(UpdateEvent::AddEdge { source_node: new_source_name.clone(), 
                                                                         target_node: new_target_name.to_string(), 
                                                                         layer: layer_name.to_string(), 
                                                                         component_type: edge_component_type.to_string(), 
                                                                         component_name: edge_component_name.to_string() })?;                           
                                // check edge for annotations that need to be transferred        
                                let edge = Edge {source: source_node, target: target}; // TODO at least for the case of pointing relations the same container might contain more than one edge, or am I wrong?                
                                for k in edge_annos.get_all_keys_for_item(&edge, None, None)? {
                                    if k.ns != ANNIS_NS {                                    
                                        let v = edge_annos.get_value_for_item(&edge, &*k)?.unwrap();  // guaranteed to exist
                                        updates.add_event(UpdateEvent::AddEdgeLabel { source_node: new_source_name.clone(), 
                                                                                      target_node: new_target_name.to_string(), 
                                                                                      layer: layer_name.to_string(), 
                                                                                      component_type: edge_component_type.to_string(), 
                                                                                      component_name: edge_component_name.to_string(), 
                                                                                      anno_ns: k.ns.to_string(), 
                                                                                      anno_name: k.name.to_string(), 
                                                                                      anno_value: v.to_string() })?;
                                    }
                                }
                                updates.add_event(UpdateEvent::DeleteEdge { source_node: source_node_name.to_string(), 
                                                                            target_node: target_name.to_string(), 
                                                                            layer: layer_name.to_string(),
                                                                            component_type: edge_component_type.to_string(), 
                                                                            component_name: edge_component_name.to_string() })?;
                            } else {
                                // child does not exist in target graph, which must be legal if we allow texts to only partially match (e. g. in the case of dropped punctuation)
                                // it could also be the case that the source and target node are not in the node_map bc they are not ordered nodes and thus don't require to be modified
                                report_missing.insert(node_annos.get_value_for_item(&target, &NODE_NAME_KEY)?.unwrap());
                            }
                        }
                        if !report_missing.is_empty() && tx.is_some() {
                            let sender = tx.as_ref().unwrap();
                            sender.send(StatusMessage::Info(format!("Not all children of node {} ({}::{}) available in target graph: {:?}", &source_node_name, &layer_name, &edge_component_name, &report_missing)))?;
                        }
                    }
                }
            }
        }
        if docs_with_errors.len() > 0 {
            let docs_s = docs_with_errors.iter().join("\n");            
            if let Some(sender) = &tx {
                let message = match on_error {
                    OnErrorValues::Fail => {
                        let msg = format!("Documents with ill-merged tokens:\n{}", docs_s);
                        let err = AnnattoError::Manipulator { reason: msg, manipulator: self.module_name().to_string() };
                        StatusMessage::Failed(err)
                    },
                    OnErrorValues::Drop => {
                        for doc_node_id in docs_with_errors {
                            updates.add_event(UpdateEvent::DeleteNode { node_name: doc_node_id })?;  //FIXME this is currently only the document name, not the entire path and thus does not identify the node
                        };
                        let msg = format!("Documents with ill-merged tokens will be dropped from the corpus:\n{}", docs_s);
                        StatusMessage::Warning(msg)
                    },
                    _ => {
                        let msg = format!("BE AWARE that the corpus contains severe merging issues in the following documents:\n{}", docs_s);
                        StatusMessage::Warning(msg)
                    }
                };
                sender.send(message)?;
            }
        }
        graph.apply_update(&mut updates, |_msg| {})?;        
        Ok(())
    }
}

const MODULE_NAME: &str = "Merger";

impl Module for Merger {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}