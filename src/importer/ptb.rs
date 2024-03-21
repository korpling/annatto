use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use encoding_rs_io::DecodeReaderBytes;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use serde_derive::Deserialize;
use std::{io::Read, path::Path};

use crate::{
    progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files, StepID,
};

use super::Importer;

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

impl<'a> DocumentMapper {
    fn map(&mut self, u: &mut GraphUpdate, mut ptb: Pairs<'a, Rule>) -> anyhow::Result<()> {
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

        // Iterate over all root phrases and map them
        if let Some(ptb) = ptb.next() {
            if ptb.as_rule() == Rule::ptb {
                for root_phrase in ptb.into_inner() {
                    if root_phrase.as_rule() == Rule::phrase {
                        self.consume_phrase(root_phrase.into_inner(), None, u)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn annotate_edge(
        &self,
        u: &mut GraphUpdate,
        source: Option<&String>,
        target: String,
        edge_label: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(parent_node_name) = source {
            // Add a a typed (with component name) and an untyped
            // dominance edge (empty component name) between this parent
            // node and the child node.
            // TODO: make the layer and component name configurable
            u.add_event(UpdateEvent::AddEdge {
                source_node: parent_node_name.to_string(),
                target_node: target.to_string(),
                layer: "syntax".to_string(),
                component_type: AnnotationComponentType::Dominance.to_string(),
                component_name: "".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: parent_node_name.clone(),
                target_node: target.to_string(),
                layer: "syntax".to_string(),
                component_type: AnnotationComponentType::Dominance.to_string(),
                component_name: "edge".to_string(),
            })?;
            if let Some(label) = edge_label {
                if !label.is_empty() {
                    u.add_event(UpdateEvent::AddEdgeLabel {
                        source_node: parent_node_name.to_string(),
                        target_node: target.to_string(),
                        layer: "syntax".to_string(),
                        component_type: AnnotationComponentType::Dominance.to_string(),
                        component_name: "".to_string(),
                        anno_ns: "syntax".to_string(),
                        anno_name: "func".to_string(),
                        anno_value: label.to_string(),
                    })?;
                    u.add_event(UpdateEvent::AddEdgeLabel {
                        source_node: parent_node_name.to_string(),
                        target_node: target,
                        layer: "syntax".to_string(),
                        component_type: AnnotationComponentType::Dominance.to_string(),
                        component_name: "edge".to_string(),
                        anno_ns: "syntax".to_string(),
                        anno_name: "func".to_string(),
                        anno_value: label.to_string(),
                    })?;
                }
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
        u: &mut GraphUpdate,
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

                u.add_event(UpdateEvent::AddNode {
                    node_name: node_name.clone(),
                    node_type: "node".to_string(),
                })?;
                // TODO: make the annotaton name configurable
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.clone(),
                    anno_ns: "syntax".to_string(),
                    anno_name: "cat".to_string(),
                    anno_value: node_label.to_string(),
                })?;
                // TODO: make the layer configurable
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "layer".to_string(),
                    anno_value: "syntax".to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: self.doc_path.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;

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
        u: &mut GraphUpdate,
        pair: &Pair<Rule>,
        phrase_label: String,
        parent: Option<&String>,
    ) -> anyhow::Result<String> {
        let value = self.consume_value(pair)?;
        let id = self.number_of_token + 1;
        let tok_id = format!("{}#t{id}", self.doc_path);
        let (node_label, edge_label) = self.split_annotation_value(&phrase_label);
        u.add_event(UpdateEvent::AddNode {
            node_name: tok_id.clone(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: value,
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: tok_id.clone(),
            target_node: self.text_node_name.clone(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        self.annotate_edge(u, parent, tok_id.to_string(), edge_label)?;
        // TODO: allow to customize the token annotation name
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: DEFAULT_NS.to_string(),
            anno_name: "pos".to_string(),
            anno_value: node_label.to_string(),
        })?;
        if let Some(last_token_id) = &self.last_token_id {
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_id.clone(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok-whitespace-before".to_string(),
                anno_value: " ".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: last_token_id.clone(),
                target_node: tok_id.clone(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
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
#[derive(Default, Deserialize, Documented, DocumentedFields)]
#[serde(default, deny_unknown_fields)]
pub struct ImportPTB {
    edge_delimiter: Option<String>,
}

const FILE_EXTENSIONS: [&str; 1] = ["ptb"];

impl Importer for ImportPTB {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();

        let documents = import_corpus_graph_from_files(&mut u, input_path, self.file_extensions())?;

        let reporter = ProgressReporter::new(tx, step_id, documents.len())?;

        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            let ptb: Pairs<Rule> = PtbParser::parse(Rule::ptb, &file_content)?;

            let text_node_name = format!("{}#text", &doc_path);

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
        Ok(u)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

#[cfg(test)]
mod tests;
