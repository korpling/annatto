use std::collections::HashSet;
use std::fmt::format;
use std::hash::Hash;
use std::iter::FromIterator;
use std::{iter::Map, collections::BTreeMap};
use crate::{Manipulator,Module,workflow::StatusSender};
use crate::error::AnnattoError;
use graphannis::update::{GraphUpdate,UpdateEvent};
use graphannis::{AnnotationGraph,CorpusStorage, graph::Match};
use graphannis_core::{annostorage::ValueSearch,errors::GraphAnnisCoreError,graph::NODE_NAME_KEY,types::AnnoKey};
use itertools::{Itertools, Update};
use pyo3::panic;
use smartstring;

pub struct CheckingMergeFinalizer {}


impl Default for CheckingMergeFinalizer {
    fn default() -> Self {
        CheckingMergeFinalizer {}
    }
}

fn split_qname(qname: &str) -> (&str, &str) {
    let split = qname.split(SEP_QNAME).collect::<Vec<&str>>();
    let mut ns;
    let name;
    match split.len() {                
        2 => {
            ns = *split.get(0).unwrap();
            name = *split.get(1).unwrap();
        },
        _ => {
            ns = "";
            name = *split.get(0).unwrap();
        }
    };
    (ns, name)
}

const PROP_CHECK_NAMES: &str = "check.names";
const PROP_KEEP_NAME: &str = "keep.name";
const SEP_QNAME: &str = "::";


impl Manipulator for CheckingMergeFinalizer {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> { // TODO this so far only implements Merge, Align is not implemented yet        
        let mut qnames = properties.get(PROP_CHECK_NAMES).unwrap().split(";").collect::<Vec<&str>>();                
        let search_name_tuples = qnames.iter().map(|qn| split_qname(qn)).collect::<Vec<(&str, &str)>>();        
        let node_annos = graph.get_node_annos();
        // variables for removing obsolete keys
        let keep_name = properties.get(PROP_KEEP_NAME).unwrap();
        let split_keep_name = split_qname(keep_name.as_str());
        let mut updates = GraphUpdate::default();
        // get to work
        let mut docs_with_errors = HashSet::new();
        for result in node_annos.exact_anno_search(Some("annis"), "tok", ValueSearch::Any) {
            let m = result?;
            let node_id = m.node;
            let node_name = node_annos.get_value_for_item(&node_id, &NODE_NAME_KEY)?.unwrap();
            let doc_name = String::from(node_name.split("#").collect_tuple::<(&str, &str)>().unwrap().0);
            if docs_with_errors.contains(&doc_name) {
                continue;
            }
            let mut anno_values = search_name_tuples.iter()
            .map(|tpl| node_annos.get_value_for_item(&node_id, &AnnoKey {ns: smartstring::alias::String::from(tpl.0), name: smartstring::alias::String::from(tpl.1)}));
            let mut values = HashSet::new();
            for result in anno_values {
                let o = result?;
                if o.is_none() {
                    continue;
                }
                let v = o.unwrap();
                values.insert(v);
            }
            if values.len() > 1 {
                docs_with_errors.insert(doc_name);
            }

            for name_tuple in &search_name_tuples {
                let ns = name_tuple.0;
                let name = name_tuple.1;
                if !name.eq(split_keep_name.1) || !ns.eq(split_keep_name.0) {
                    updates.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), anno_ns: String::from(ns), anno_name: String::from(name) })?;
                }
            }
        };
        if docs_with_errors.len() > 0 {
            let msg = docs_with_errors.iter().join("\n");
            return Err(Box::new(AnnattoError::Manipulator { reason: msg, manipulator: String::from(self.module_name()) }));
        }   
        graph.apply_update(&mut updates, |_msg| {})?;
        Ok(())
    }
}

impl Module for CheckingMergeFinalizer {
    fn module_name(&self) -> &str {
        "CheckingMergeFinalizer"
    }
}