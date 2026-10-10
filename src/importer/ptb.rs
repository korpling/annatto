use anyhow::anyhow;
use encoding_rs_io::DecodeReaderBytes;
use facet::Facet;
use graphannis::model::{AnnotationComponent, AnnotationComponentType};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use lazy_static::lazy_static;
use pest::{
    Parser,
    iterators::{Pair, Pairs},
};
use pest_derive::Parser;
use serde::Serialize;
use serde_derive::Deserialize;
use std::{io::Read, path::Path};

use crate::{
    StepID,
    importer::{DefaultImportConfiguration, GenericImportConfiguration},
    progress::ProgressReporter,
    util::graphupdate::UpdateWriter,
};

use super::Importer;

/// This implements the Pest parser for the given grammar.
#[derive(Parser)]
#[grammar = "importer/ptb/ptb.pest"]
pub struct PtbParser;

struct DocumentMapper {
    doc_path: String,
    text_node_name: String,
    last_token_id: Option<String>,
    number_of_token: usize,
    number_of_spans: usize,
    edge_delimiter: Option<String>,
}

impl DocumentMapper {
    fn map(&mut self, u: &mut UpdateWriter, mut ptb: Pairs<Rule>) -> anyhow::Result<()> {
        // Add a subcorpus like node for the text
        u.node(self.text_node_name.clone(), "datasource".to_string())?;
        u.part_of(self.text_node_name.clone(), self.doc_path.clone())?;

        // Iterate over all root phrases and map them
        if let Some(ptb) = ptb.next()
            && ptb.as_rule() == Rule::ptb
        {
            for root_phrase in ptb.into_inner() {
                if root_phrase.as_rule() == Rule::phrase {
                    self.consume_phrase(root_phrase.into_inner(), None, u)?;
                }
            }
        }
        Ok(())
    }

    fn annotate_edge(
        &self,
        u: &mut UpdateWriter,
        source: Option<&String>,
        target: String,
        edge_label: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(parent_node_name) = source {
            // Add a a typed (with component name) and an untyped
            // dominance edge (empty component name) between this parent
            // node and the child node.
            // TODO: make the layer and component name configurable
            let prim_c = u
                .dominance(parent_node_name.to_string(), target.to_string())?
                .clone();
            let sec_c = u.edge_by_parts(
                parent_node_name.clone(),
                target.to_string(),
                u.config.default_dominance().layer.to_string(),
                u.config.default_dominance().get_type(),
                format!("{}edge", u.config.default_dominance().name),
            )?;
            if let Some(label) = edge_label
                && !label.is_empty()
            {
                u.edge_label(
                    parent_node_name.to_string(),
                    target.to_string(),
                    &prim_c,
                    u.config.default_namespace().to_string(),
                    "func".to_string(),
                    label.to_string(),
                )?;
                u.edge_label(
                    parent_node_name.to_string(),
                    target.to_string(),
                    &sec_c,
                    u.config.default_namespace().to_string(),
                    "func".to_string(),
                    label.to_string(),
                )?;
            }
        }
        Ok(())
    }

    fn split_annotation_value<'b>(&self, phrase_label: &'b str) -> (&'b str, Option<&'b str>) {
        if let Some(sep) = &self.edge_delimiter {
            if let Some((n, e)) = phrase_label.split_once(sep) {
                (n, Some(e))
            } else {
                (phrase_label, None)
            }
        } else {
            (phrase_label, None)
        }
    }

    fn consume_phrase(
        &mut self,
        mut phrase_children: Pairs<Rule>,
        parent: Option<&String>,
        u: &mut UpdateWriter,
    ) -> anyhow::Result<String> {
        // The first child of a phrase we want to map must be a label
        if let Some(phrase_label) = phrase_children.next() {
            let phrase_label = self.consume_label(phrase_label)?;
            let remaining_children: Vec<_> = phrase_children.collect();
            if remaining_children.len() == 1
                && (remaining_children[0].as_rule() == Rule::quoted_value
                    || remaining_children[0].as_rule() == Rule::label)
            {
                // map the value as token
                let tok_id = self.consume_token(u, &remaining_children[0], phrase_label, parent)?;
                Ok(tok_id)
            } else {
                let (node_label, edge_label) = self.split_annotation_value(&phrase_label);
                // Map this as span
                let id = self.number_of_spans + 1;
                let node_name = format!("{}#n{id}", self.doc_path);

                u.labeled_node(
                    node_name.clone(),
                    "node".to_string(),
                    u.config.default_namespace().to_string(),
                    "cat".to_string(),
                    node_label.to_string(),
                )?;
                // TODO: make the layer configurable
                u.node_label(
                    node_name.clone(),
                    ANNIS_NS.to_string(),
                    "layer".to_string(),
                    u.config.default_namespace().to_string(), // TODO re-think that for the future
                )?;
                u.part_of(node_name.to_string(), self.doc_path.to_string())?;

                self.annotate_edge(u, parent, node_name.to_string(), edge_label)?;
                self.number_of_spans += 1;

                // Left-descend to any phrase
                //let mut target_node = node_name.to_string();
                for c in remaining_children {
                    self.consume_phrase(c.into_inner(), Some(&node_name), u)?;
                }
                Ok(node_name)
            }
        } else {
            Err(anyhow!("Empty phrase without label or children"))
        }
    }

    fn consume_token(
        &mut self,
        u: &mut UpdateWriter,
        pair: &Pair<Rule>,
        phrase_label: String,
        parent: Option<&String>,
    ) -> anyhow::Result<String> {
        let value = self.consume_value(pair)?;
        let id = self.number_of_token + 1;
        let tok_id = format!("{}#t{id}", self.doc_path);
        let (node_label, edge_label) = self.split_annotation_value(&phrase_label);
        u.labeled_node(
            tok_id.clone(),
            "node".to_string(),
            ANNIS_NS.to_string(),
            "tok".to_string(),
            value,
        )?;
        u.node_label(
            tok_id.clone(),
            ANNIS_NS.to_string(),
            "layer".to_string(),
            "default_layer".to_string(),
        )?;
        u.part_of(tok_id.clone(), self.text_node_name.clone())?;
        self.annotate_edge(u, parent, tok_id.to_string(), edge_label)?;
        // TODO: allow to customize the token annotation name
        u.node_label(
            tok_id.clone(),
            u.config.default_namespace().to_string(),
            "pos".to_string(),
            node_label.to_string(),
        )?;
        if let Some(last_token_id) = &self.last_token_id {
            u.node_label(
                tok_id.clone(),
                ANNIS_NS.to_string(),
                "tok-whitespace-before".to_string(),
                " ".to_string(),
            )?;
            u.ordering(last_token_id.clone(), tok_id.clone())?;
        }
        self.number_of_token += 1;
        self.last_token_id = Some(tok_id.clone());
        Ok(tok_id)
    }

    fn consume_value(&self, value: &Pair<Rule>) -> anyhow::Result<String> {
        let r = value.as_rule();
        if r == Rule::label {
            // Replace any special bracket value
            let value = value.as_str().replace("-LRB-", "(").replace("-RRB-", ")");
            Ok(value)
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
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportPTB {
    /// Some variants encode a function of the node after the node category,
    /// such as "NP-subj", which means this nominal phrase node has the function
    /// of the subject. If "-" is provided as `edge_delimiter`, the node
    /// will carry a category "NP", whereas the ingoing edge will have a
    /// function label "subj".
    #[serde(default)]
    edge_delimiter: Option<String>,
}

const FILE_EXTENSIONS: [&str; 1] = ["ptb"];

lazy_static! {
    static ref DEFAULT_DOMINANCE_COMPONENT: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Dominance,
        "syntax".to_string(),
        "".to_string()
    );
}

impl DefaultImportConfiguration for ImportPTB {
    fn default_file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }

    fn preset_default_namespace(&self) -> Option<&str> {
        Some("syntax")
    }

    fn preset_default_dominance(&self) -> Option<&AnnotationComponent> {
        Some(&DEFAULT_DOMINANCE_COMPONENT)
    }
}

impl Importer for ImportPTB {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        config: GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = UpdateWriter::from(config);

        let documents = u.write_corpus_graph_from_file_tree(input_path)?;

        let reporter = ProgressReporter::new(tx, step_id, documents.len())?;

        let mut files_with_errors = Vec::default();

        for (file_path, doc_path) in documents {
            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            match PtbParser::parse(Rule::ptb, file_content.trim()) {
                Ok(ptb) => {
                    let text_node_name = format!("{doc_path}#text");

                    let mut doc_mapper = DocumentMapper {
                        doc_path,
                        text_node_name,
                        last_token_id: None,
                        number_of_token: 0,
                        number_of_spans: 0,
                        edge_delimiter: self.edge_delimiter.clone(),
                    };

                    doc_mapper.map(&mut u, ptb)?;
                    reporter.worked(1)?;
                }
                Err(e) => files_with_errors.push((file_path, e)),
            };
        }
        if !files_with_errors.is_empty() {
            let msg = files_with_errors
                .into_iter()
                .map(|(p, e)| format!("\nCould not parse {p:?}: {}", e))
                .join("");
            Err(anyhow!(msg).into())
        } else {
            Ok(u.into())
        }
    }
}

#[cfg(test)]
mod tests;
