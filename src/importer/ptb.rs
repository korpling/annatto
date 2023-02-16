use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use encoding_rs_io::DecodeReaderBytes;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;
use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;

use crate::{
    progress::ProgressReporter,
    util::graphupdate::{path_structure, root_corpus_from_path},
    Module,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_ptb";

#[derive(Parser)]
#[grammar = "importer/ptb/ptb.pest"]
pub struct PtbParser;

struct DocumentMapper<'a> {
    root_corpus: String,
    doc_path: String,
    text_node_name: String,
    reporter: &'a ProgressReporter,
    file_path: PathBuf,
}

impl<'a> DocumentMapper<'a> {
    fn map(&mut self, u: &mut GraphUpdate, ptb: Pairs<'a, Rule>) -> anyhow::Result<()> {
        // Add a subcorpus like node for the text
        u.add_event(UpdateEvent::AddNode {
            node_name: self.text_node_name.clone(),
            node_type: "datasource".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: self.text_node_name.clone(),
            target_node: self.doc_path.clone(),
            layer: ANNIS_NS.to_string(),
            component_type: "PartOf".to_string(),
            component_name: "".to_string(),
        })?;

        // Iterate over all root spans and map these sentences
        for pair in ptb {
            if Rule::root == pair.as_rule() {
                self.consume_root(pair.into_inner())?
            }
        }
        Ok(())
    }

    fn consume_root(&self, mut root_children: Pairs<Rule>) -> anyhow::Result<()> {
        // A root must have exactly one phrase child
        if let Some(phrase) = root_children.next() {
            if phrase.as_rule() == Rule::phrase {
                self.consume_phrase(phrase.into_inner())?;
                Ok(())
            } else {
                Err(anyhow!(
                    "Expected phrase but got {:?} ({:?})",
                    phrase.as_rule(),
                    phrase.as_span()
                ))
            }
        } else {
            Err(anyhow!("Missing phrase for root element"))
        }
    }

    fn consume_phrase(&self, mut phrase_children: Pairs<Rule>) -> anyhow::Result<()> {
        // First child element of a phrase must be a label
        if let Some(phrase_label) = phrase_children.next() {
            let phrase_label = self.consume_label(phrase_label)?;

            let children: Vec<_> = phrase_children.collect();
            if children.len() == 1 {
                // TODO: map the token value
                let value = self.consume_value(&children[0])?;
            } else if children.len() > 1 {
                // TODO: Left-descend to any phrase
            }
        }
        Ok(())
    }

    fn consume_value(&self, value: &Pair<Rule>) -> anyhow::Result<String> {
        let r = value.as_rule();
        if r == Rule::label {
            Ok(value.as_str().to_string())
        } else if r == Rule::quoted_value {
            let raw_value = value.as_str();
            // Remove the quotation marks at the beginning and end
            Ok(raw_value[1..raw_value.len() - 1].to_string())
        } else {
            Err(anyhow!(
                "Expected (quoted) value but got {:?} ({:?})",
                r,
                value.as_span()
            ))
        }
    }

    fn consume_label(&self, label: Pair<Rule>) -> anyhow::Result<String> {
        if label.as_rule() == Rule::label {
            Ok(label.as_str().to_string())
        } else {
            Err(anyhow!(
                "Expected label but got {:?} ({:?})",
                label.as_rule(),
                label.as_span()
            ))
        }
    }
}

/// Importer the Penn Treebank Bracketed Text format (PTB)
#[derive(Default)]
pub struct PtbImporter {}

impl Module for PtbImporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for PtbImporter {
    fn import_corpus(
        &self,
        input_path: &Path,
        properties: &BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();

        let documents = path_structure(&mut u, input_path, &["ptb"])?;

        let reporter =
            ProgressReporter::new(tx, self as &dyn Module, Some(input_path), documents.len())?;

        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            let ptb: Pairs<Rule> = PtbParser::parse(Rule::ptb, &file_content)?;

            let text_node_name = format!("{}#text", &doc_path);
            let root_corpus = root_corpus_from_path(input_path)?;

            let mut doc_mapper = DocumentMapper {
                root_corpus,
                doc_path,
                reporter: &reporter,
                file_path,
                text_node_name,
            };

            doc_mapper.map(&mut u, ptb)?;
            reporter.worked(1)?;
        }
        Ok(u)
    }
}

#[cfg(test)]
mod tests;
