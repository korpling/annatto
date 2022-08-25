use std::collections::HashSet;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use crate::{Manipulator,Module,workflow::StatusSender};
use crate::error::AnnattoError;
use graphannis::update::{GraphUpdate,UpdateEvent};
use graphannis::AnnotationGraph;
use graphannis_core::{annostorage::ValueSearch,graph::NODE_NAME_KEY,types::AnnoKey};
use itertools::Itertools;
use smartstring;

pub struct CheckingMergeFinalizer {}


impl Default for CheckingMergeFinalizer {
    fn default() -> Self {
        CheckingMergeFinalizer {}
    }
}

fn split_qname(qname: &str) -> (&str, &str) {
    let split = qname.split(SEP_QNAME).collect::<Vec<&str>>();
    let ns;
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
            _ => Err(AnnattoError::Manipulator { reason: format!("Undefined value for property {}: {}", PROP_ON_ERROR, value), manipulator: String::from(MODULE_NAME) })
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
        let on_error = OnErrorValues::try_from(properties.get(PROP_ON_ERROR))?;
        let qnames = properties.get(PROP_CHECK_NAMES).unwrap().split(";").collect::<Vec<&str>>();                
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
            let anno_values = search_name_tuples.iter()
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
                continue;  // no updates for faulty documents
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