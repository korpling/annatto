use std::{collections::BTreeMap, convert::TryFrom};

use graphannis::update::GraphUpdate;
use itertools::Itertools;

use crate::Module;

use super::Importer;

pub const MODULE_NAME: &str = "import_conll";

pub struct ImportCoNLL {}

impl Default for ImportCoNLL {
    fn default() -> Self {
        Self {}
    }
}

impl Module for ImportCoNLL {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for ImportCoNLL {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();

        Ok(update)
    }
}

impl ImportCoNLL {
    fn import_document(
        &self,
        update: &mut GraphUpdate,
        corpus_path: &str,
        document_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

struct CoNLL {
    sentences: Vec<CoNLLSentence>,
}

struct CoNLLSentence {
    tokens: Vec<CoNLLToken>,
}

struct CoNLLToken {
    id: usize,
    form: Option<String>,
    lemma: Option<String>,
    upos: Option<String>,
    xpos: Option<String>,
    features: BTreeMap<String, String>,
    head: Option<usize>,
    deprel: Option<String>,
    deps: BTreeMap<usize, String>,
    misc: BTreeMap<String, String>,
}

fn 

impl TryFrom<String> for CoNLLToken {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let entries = value.splitn(10, "\t").collect_vec();
        Ok(CoNLLToken {
            id: entries[0].parse::<usize>()?,
            form: Some(entries[1].to_string()),
            lemma: Some(entries[2].to_string()),
            upos: Some(entries[3].to_string()),
            xpos: Some(entries[4].to_string()),
            features: entries[5]
                .split("|")
                .map(|e| e.split_once("=").unwrap())
                .map(|e| (e.0.to_string(), e.1.to_string()))
                .collect::<BTreeMap<String, String>>(),
            head: Some(entries[6].parse::<usize>()?),
            deprel: Some(entries[7].to_string()),
            deps: entries[8]
                .split("|")
                .map(|e| e.split_once(":").unwrap())
                .map(|e| {
                    (
                        match e.0.parse::<usize>() {
                            Ok(v) => v,
                            Err(_) => 0,
                        },
                        e.1.to_string(),
                    )
                })
                .collect::<BTreeMap<usize, String>>(),
            misc: entries[9]
                .split("|")
                .map(|e| e.split_once("=").unwrap())
                .map(|e| (e.0.to_string(), e.1.to_string()))
                .collect::<BTreeMap<String, String>>(),
        })
    }
}
