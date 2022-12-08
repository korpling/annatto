use std::collections::{HashMap,HashSet,BTreeMap,Bound};
use std::convert::TryFrom;
use std::fmt::format;
use std::hash::Hash;
use std::iter::FromIterator;
use crate::{Manipulator,Module,workflow::StatusSender};
use crate::error::AnnattoError;
use crate::workflow::StatusMessage;
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
const SEP_QNAME: &str = "::";


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
            sender.send(StatusMessage::Info(String::from("Starting merge check")))?;
        }
        let on_error = OnErrorValues::try_from(properties.get(PROP_ON_ERROR))?;
        let order_names = properties.get(PROP_CHECK_NAMES).unwrap().split(",").collect::<Vec<&str>>();    
        let node_annos = graph.get_node_annos();
        // variables for removing obsolete keys
        let keep_name = properties.get(PROP_KEEP_NAME).unwrap();
        let keep_name_key = AnnoKey { ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(keep_name) };        
        let mut updates = GraphUpdate::default();
        // gather ordered tokens
        let mut docs_with_errors = HashSet::new();
        let mut ordered_items_by_name: HashMap<&str, Vec<u64>> = HashMap::new();
        let mut index_by_name: HashMap<&str, usize> = HashMap::new();
        for o_n in &order_names {
            let order_name = *o_n;
            let c = AnnotationComponent::new(AnnotationComponentType::Ordering, smartstring::alias::String::from(ANNIS_NS), smartstring::alias::String::from(order_name));            
            if let Some(ordering) = graph.get_graphstorage(&c) {
                let name_matches = node_annos.exact_anno_search(None, order_name, ValueSearch::Any).collect_vec();
                if let Some(sender) = &tx {
                    let message = format!("Found {} matching nodes for name {}", name_matches.len(), order_name);
                    sender.send(StatusMessage::Info(message));
                }
                let start_node = name_matches.into_iter().min_by(|r, r_| r.as_ref().unwrap().node.cmp(&r_.as_ref().unwrap().node)).unwrap()?.node;
                let in_edges = ordering.get_ingoing_edges(start_node).count();
                if in_edges > 0 {
                    panic!("Could not determine root of ordering."); //TODO
                }                
                let mut nodes: Vec<u64> = [].to_vec();
                for entry in ordering.find_connected(start_node, 1, Bound::Excluded(usize::MAX)) {
                    nodes.push(entry?);
                }
                ordered_items_by_name.insert(order_name, nodes);
                index_by_name.insert(order_name, 0);
            } else {
               panic!("No ordering with name {}", order_name); //TODO
            }            
        }
        // set up some trackers
        let mut node_map: HashMap<&u64, &u64> = HashMap::new();  // maps node ids of old annotations owners to new (remaining) annotation owners, relevant for mapping edges later
        // merge or align                
        for item in ordered_items_by_name.get(keep_name.as_str()).unwrap() {
            let ref_val = node_annos.get_value_for_item(item, &keep_name_key)?.unwrap();  // by definition this has to exist
            let ref_node_name = node_annos.get_value_for_item(item, &NODE_NAME_KEY)?.unwrap();  // existence guaranteed
            for name in &order_names {
                let other_name = *name;
                if other_name.eq(keep_name) {
                    continue;
                }
                let i = index_by_name.get(other_name).unwrap();
                if let Some(other_item) = ordered_items_by_name.get(other_name).unwrap().get(*i) {
                    let other_key = AnnoKey {ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(other_name)};
                    let other_val = node_annos.get_value_for_item(other_item, &other_key)?.unwrap();
                    if (*ref_val).eq(&*other_val) {  // text values match
                        // align or merge
                        // case merge
                        let anno_keys = node_annos.get_all_keys_for_item(other_item, None, None)?;
                        // annotations directly on the ordered node
                        for ak in anno_keys {
                            let av = node_annos.get_value_for_item(other_item, &*ak)?.unwrap();  // existence guaranteed
                            updates.add_event(UpdateEvent::AddNodeLabel { node_name: String::from(&*ref_node_name), anno_ns: ak.ns.to_string(), anno_name: ak.name.to_string(), anno_value: av.to_string()})?;
                        }
                        // delete ordered node, the rest (edges and labels) should theoretically die as a consequence
                        let other_node_name = node_annos.get_value_for_item(other_item, &NODE_NAME_KEY)?.unwrap().to_string();  // existence guaranteed
                        updates.add_event(UpdateEvent::DeleteNode { node_name: other_node_name })?;
                        node_map.insert(other_item, item);
                    } else {  // text values don't match
                        // alternative 
                        // TODO implement logic for punctuation etc, for now just fail
                        panic!("Could not merge target text {} with text {}", keep_name.as_str(), other_name);  // TODO
                    }                    
                } else {
                    // no further nodes
                }
                index_by_name.insert(other_name, i + 1);
            }
        }
        // do edges
        for edge_component_type in [AnnotationComponentType::Coverage, 
                                                             AnnotationComponentType::Dominance, 
                                                             AnnotationComponentType::Pointing] {            
            for edge_component in graph.get_all_components(Some(edge_component_type), None) {            
                let edge_component_name = &edge_component.name;
                if let Some(edge_storage) = graph.get_graphstorage(&edge_component) {
                    // there are some coverage edges
                    let edge_annos = edge_storage.get_anno_storage();
                    for source_node_r in edge_storage.source_nodes() {
                        let source_node = source_node_r?;
                        let source_node_name = node_annos.get_value_for_item(&source_node, &NODE_NAME_KEY)?.unwrap(); // existence guaranteed
                        for target_r in edge_storage.find_connected(source_node, 1, Bound::Included(1)) {
                            let target = target_r?;                    
                            if let Some(new_child) = node_map.get(&target) {                            
                                // new child still exists in target graph
                                let target_name = node_annos.get_value_for_item(&target, &NODE_NAME_KEY)?.unwrap();  // existence guaranteed                        
                                let layer_name = edge_component.layer.to_string();                        
                                updates.add_event(UpdateEvent::DeleteEdge { source_node: String::from(&*source_node_name), 
                                                                            target_node: String::from(target_name), 
                                                                            layer: layer_name.to_string(),
                                                                            component_type: edge_component_type.clone().to_string(), 
                                                                            component_name: edge_component_name.to_string()})?;
                                let new_target_name = node_annos.get_value_for_item(*new_child, &NODE_NAME_KEY)?.unwrap();
                                updates.add_event(UpdateEvent::AddEdge { source_node: String::from(&*source_node_name), 
                                                                        target_node: String::from(new_target_name.clone()), 
                                                                        layer: layer_name.to_string(), 
                                                                        component_type: edge_component_type.clone().to_string(), 
                                                                        component_name: edge_component_name.to_string()})?;
                                // check edge for annotations that need to be transferred        
                                let edge = Edge {source: source_node, target: target}; // TODO at least for the case of pointing relations the same container might contain more than one edge, or am I wrong?                
                                for k in edge_annos.get_all_keys_for_item(&edge, None, None)? {
                                    let v = edge_annos.get_value_for_item(&edge, &*k)?.unwrap();  // guaranteed to exist
                                    updates.add_event(UpdateEvent::AddEdgeLabel { source_node: String::from(&*source_node_name), 
                                                                                target_node: String::from(new_target_name.clone()), 
                                                                                layer: layer_name.to_string(), 
                                                                                component_type: edge_component_type.clone().to_string(), 
                                                                                component_name: edge_component_name.to_string(), 
                                                                                anno_ns: k.ns.to_string(), 
                                                                                anno_name: k.name.to_string(), 
                                                                                anno_value: String::from(v) })?;
                                }
                            } else {
                                // child does not exist in target graph, which must be legal if we allow texts to only partially match (e. g. in the case of dropped punctuation)
                                // it could also be the case that the source and target node are not in the node_map bc they are not ordered nodes and thus don't require to be modified
                                if let Some(sender) = &tx {
                                    sender.send(StatusMessage::Info(format!("Not all children of node {} available in target graph.", &source_node)))?;
                                }
                            }
                        }
                    }
                }
            }
        }
        if docs_with_errors.len() > 0 {
            let msg = docs_with_errors.iter().join("\n");
            if let Some(sender) = &tx {
                sender.send(StatusMessage::Warning(format!("Documents with ill-merged tokens:\n{}", msg)))?;
            }
            match on_error {
                OnErrorValues::Fail => return Err(Box::new(AnnattoError::Manipulator { reason: String::from("Mismatching tokens in some documents."), manipulator: String::from(self.module_name()) })),
                OnErrorValues::Drop => {
                    for doc_node_id in docs_with_errors {
                        updates.add_event(UpdateEvent::DeleteNode { node_name: doc_node_id })?;
                    }
                },
                _ => {}
            };
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