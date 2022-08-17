use std::collections::HashSet;
use std::fmt::format;
use std::iter::FromIterator;
use std::{iter::Map, collections::BTreeMap};
use crate::{Manipulator,Module,workflow::StatusSender};
use crate::error::AnnattoError;
use graphannis::update::{GraphUpdate,UpdateEvent};
use graphannis::{AnnotationGraph,CorpusStorage, graph::Match};
use graphannis_core::{annostorage::ValueSearch,errors::GraphAnnisCoreError,graph::NODE_NAME_KEY};
use itertools::{Itertools, Update};
use pyo3::panic;


pub struct CheckingMergeFinalizer {}


impl Default for CheckingMergeFinalizer {
    fn default() -> Self {
        CheckingMergeFinalizer {}
    }
}

fn split_qname(qname: &str) -> (Option<&str>, &str) {
    let split = qname.split("::").collect::<Vec<&str>>();
    let mut ns;
    let name;
    match split.len() {                
        2 => {
            ns = Some(*split.get(0).unwrap());
            name = *split.get(1).unwrap();
        },
        _ => {
            ns = None;
            name = *split.get(0).unwrap();
        }
    };
    (ns, name)
}


const PROP_CHECK_NAMES: &str = "check.names";
const PROP_KEEP_NAME: &str = "keep.name";


impl Manipulator for CheckingMergeFinalizer {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> { // TODO this so far only implements Merge, Align is not implemented yet        
        let qnames = properties.get(PROP_CHECK_NAMES).unwrap().split(";").collect::<Vec<&str>>();
        let n = qnames.len();
        let node_annos = graph.get_node_annos();
        // variables for removing obsolete keys
        let prop_names = properties.get(PROP_CHECK_NAMES);
        let keep_name = properties.get(PROP_KEEP_NAME).unwrap();
        let mut updates = GraphUpdate::default();        
        for qname_pair in qnames.into_iter().combinations(2) {
            let first = *qname_pair.first().unwrap();
            let ns_name_1 = split_qname(first);
            let ns_1 = ns_name_1.0;
            let name_1 = ns_name_1.1;
            let second = qname_pair.get(1).unwrap();
            let ns_name_2 = split_qname(second);
            let ns_2 = ns_name_2.0;
            let name_2 = ns_name_2.1;
            let results_1 = node_annos.exact_anno_search(ns_1, name_1, ValueSearch::Any);
            let results_2 = node_annos.exact_anno_search(ns_2, name_2, ValueSearch::Any);            
            let mut comparable_map = BTreeMap::new();
            for entry in results_2 {
                let e = entry?;
                let key = e.node;
                comparable_map.insert(key, e);
            }
            for entry in results_1 {
                // compare if values are equal?
                let e = entry?;
                let k = e.node;
                let v = e.anno_key;
                let v_ = &comparable_map.get(&k).unwrap().anno_key;
                let av_1 = graph.get_node_annos().get_value_for_item(&k, &*v)?.unwrap();
                let av_2 = graph.get_node_annos().get_value_for_item(&k, &*v_)?.unwrap();
                let node_name = node_annos.get_value_for_item(&k, &NODE_NAME_KEY).unwrap().unwrap();
                if !(*av_1).eq(&*av_2) {
                    let error_msg = format!("Text values for node {} do not match: {} != {}", &node_name, av_1, av_2);
                    return Err(Box::new(AnnattoError::Manipulator { reason: String::from(error_msg), manipulator: String::from(self.module_name()) }));
                }
                // delete node annotations that are no longer required
                if !(*v).name.as_str().eq(&keep_name[..]) {
                    let namespace = match ns_1 {
                        None => String::new(),
                        Some(s) => String::from(s)
                    };
                    updates.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), anno_ns: namespace, anno_name: String::from(name_1) })?;
                }
                if !(*v_).name.as_str().eq(&keep_name[..]) {
                    let namespace = match ns_2 {
                        None => String::new(),
                        Some(s) => String::from(s)
                    };
                    updates.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), anno_ns: namespace, anno_name: String::from(name_2) })?;
                }
            }
            // rename all namespaces --> right now they are not imported for the minor importers            
            // delete ordering --> right now they are not imported for the minor importers
        };
        graph.apply_update(&mut updates, |_msg| {})?;
        Ok(())
    }
}

impl Module for CheckingMergeFinalizer {
    fn module_name(&self) -> &str {
        "CheckingMergeFinalizer"
    }
}