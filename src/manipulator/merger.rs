use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::hash::Hash;
use crate::{Manipulator,Module,workflow::StatusSender};
use crate::error::AnnattoError;
use graphannis::graph::Component;
use graphannis::model::{AnnotationComponentType,AnnotationComponent};
use graphannis::update::{GraphUpdate,UpdateEvent};
use graphannis::AnnotationGraph;
use graphannis_core::errors::GraphAnnisCoreError;
use graphannis_core::graph::ANNIS_NS;
use graphannis_core::{
    annostorage::{ValueSearch,Match},
    dfs::CycleSafeDFS,
    graph::NODE_NAME_KEY,
    types::{AnnoKey,ComponentType},
    util::{join_qname,split_qname}
};
use itertools::Itertools;
use smartstring;
use itertools::Zip;

pub struct CheckingMergeFinalizer {}


impl Default for CheckingMergeFinalizer {
    fn default() -> Self {
        CheckingMergeFinalizer {}
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

impl Manipulator for CheckingMergeFinalizer {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &tx {
            sender.send(crate::workflow::StatusMessage::Info(String::from("Starting merge check")))?;
        }
        let ordered_names = graph.get_all_components(Some(AnnotationComponentType::Ordering), None);
        let on_error = OnErrorValues::try_from(properties.get(PROP_ON_ERROR))?;
        let qnames = properties.get(PROP_CHECK_NAMES).unwrap().split(";").collect::<Vec<&str>>();                        
        let node_annos = graph.get_node_annos();
        // variables for removing obsolete keys
        let keep_name = properties.get(PROP_KEEP_NAME).unwrap();
        let keep_name_key = AnnoKey { ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(keep_name) };
        let split_keep_name = split_qname(keep_name.as_str());
        let mut updates = GraphUpdate::default();
        // gather ordered tokens
        let mut docs_with_errors = HashSet::new();
        let mut ordered_items_by_name: HashMap<&str, Vec<u64>> = HashMap::new();
        let mut index_by_name: HashMap<&str, usize> = HashMap::new();
        for qname in qnames.iter() {
            let c = AnnotationComponent::new(AnnotationComponentType::Ordering, smartstring::alias::String::from(ANNIS_NS), smartstring::alias::String::from(*qname));            
            if let Some(ordering) = graph.get_graphstorage(&c) {
                let name_matches = node_annos.exact_anno_search(None, *qname, ValueSearch::Any).collect_vec();                
                let count = name_matches.len();
                let start_node = name_matches.into_iter().min_by(|r, r_| r.as_ref().unwrap().node.cmp(&r_.as_ref().unwrap().node)).unwrap()?.node;
                let in_edges = ordering.get_ingoing_edges(start_node).count();
                if in_edges > 0 {
                    panic!("Could not determine root of ordering."); //TODO
                }
                let dfs = CycleSafeDFS::new((*ordering).as_edgecontainer(), start_node, 1, count);
                let mut nodes: Vec<u64> = [].to_vec();
                for entry in dfs {
                    nodes.push(entry?.node);
                }
                ordered_items_by_name.insert(qname, nodes);
                index_by_name.insert(qname, 0);
            } else {
               panic!("No ordering with name {}", *qname); //TODO
            }            
        }
        // set up some trackers

        // merge or align        
        let other_names = qnames.into_iter().filter(|&name| !keep_name.as_str().eq(name)).collect::<Vec<&str>>();
        for item in ordered_items_by_name.get(keep_name.as_str()).unwrap() {
            let ref_val = node_annos.get_value_for_item(item, &keep_name_key)?.unwrap();  // by definition this has to exist
            let ref_node_name = node_annos.get_value_for_item(item, &NODE_NAME_KEY)?.unwrap();  // existence guaranteed
            for other_name in &other_names {
                let i = index_by_name.get(*other_name).unwrap();
                if let Some(other_item) = ordered_items_by_name.get(*other_name).unwrap().get(*i) {
                    let other_key = AnnoKey {ns: smartstring::alias::String::from(""), name: smartstring::alias::String::from(*other_name)};
                    let other_val = node_annos.get_value_for_item(other_item, &other_key)?.unwrap();
                    if (*ref_val).eq(&*other_val) {  // text values match
                        // align or merge
                        // case merge
                        let anno_keys = node_annos.get_all_keys_for_item(other_item, None, None)?;
                        // annotations directly on the ordered node
                        for ak in anno_keys {
                            let av = node_annos.get_value_for_item(other_item, &*ak)?.unwrap();  // existence guaranteed
                            updates.add_event(UpdateEvent::AddNodeLabel { node_name: String::from(&*ref_node_name), anno_ns: ak.ns.to_string(), anno_name: ak.name.to_string(), anno_value: av.to_string() });
                        }
                        // delete ordered node, the rest (edges and labels) should theoretically die as a consequence
                        let other_node_name = node_annos.get_value_for_item(other_item, &NODE_NAME_KEY)?.unwrap().to_string();  // existence guaranteed
                        updates.add_event(UpdateEvent::DeleteNode { node_name: other_node_name });
                    } else {  // text values don't match
                        // alternative 
                        // TODO implement logic for punctuation etc, for now just fail
                        panic!("Could not merge target text {} with text {}", keep_name.as_str(), *other_name);  // TODO
                    }                    
                } else {
                    // no further nodes
                }
                index_by_name.insert(*other_name, i + 1);
            }
        }
        //for result in node_annos.exact_anno_search(Some(ANNIS_NS), "tok", ValueSearch::Any) {
            // let m = result?;
            // let node_id = m.node;
            // let node_name = node_annos.get_value_for_item(&node_id, &NODE_NAME_KEY)?.unwrap();
            // let doc_name = String::from(node_name.split("#").collect_tuple::<(&str, &str)>().unwrap().0);
            // if docs_with_errors.contains(&doc_name) {
            //     continue;
            // }
            // let anno_values = search_name_tuples.iter()
            // .map(|tpl| node_annos.get_value_for_item(&node_id, &AnnoKey {ns: smartstring::alias::String::from(tpl.0), name: smartstring::alias::String::from(tpl.1)}));
            // let mut values = HashSet::new();
            // for result in anno_values {
            //     let o = result?;
            //     if o.is_none() {
            //         continue;
            //     }
            //     let v = o.unwrap();
            //     values.insert(v);
            // }
            // if values.len() > 1 {
            //     docs_with_errors.insert(doc_name);
            //     continue;  // no updates for faulty documents
            // }

            // for name_tuple in &search_name_tuples {
            //     let ns = name_tuple.0;
            //     let name = name_tuple.1;
            //     if !name.eq(split_keep_name.1) || !ns.eq(split_keep_name.0) {
            //         updates.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), anno_ns: String::from(ns), anno_name: String::from(name) })?;
            //     }
            // }
        //};
        if docs_with_errors.len() > 0 {
            let msg = docs_with_errors.iter().join("\n");
            if let Some(sender) = &tx {
                sender.send(crate::workflow::StatusMessage::Warning(format!("Documents with ill-merged tokens:\n{}", msg)))?;
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

const MODULE_NAME: &str = "CheckingMergeFinalizer";

impl Module for CheckingMergeFinalizer {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}