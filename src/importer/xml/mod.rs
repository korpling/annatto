use std::{collections::BTreeMap, fs::File, path::Path};

use facet::Facet;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use xml::{EventReader, ParserConfig};

use crate::{
    StepID,
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
};
use documented::{Documented, DocumentedFields};

use super::Importer;

/// Generic importer for XML files.
#[derive(
    Facet,
    Default,
    Deserialize,
    Documented,
    DocumentedFields,
    FieldNamesAsSlice,
    Serialize,
    Clone,
    PartialEq,
)]
#[serde(deny_unknown_fields)]
pub struct ImportXML {
    /// For specfic tag names, the covered text can be retrieved from
    /// attribute values rather than the enclosed text. This is required
    /// for unary tags, for example, especially for stand-off formats.
    /// This attribute maps tag names to attribute names.
    #[serde(default)]
    text_from_attribute: BTreeMap<String, String>,
    /// The given string value will be appended to the covered text after
    /// seeing the closing tag. A non-empty string is required to represent
    /// unary tags. This is crucial for dealing with stand-off formats.
    #[serde(default)]
    closing_default: String,
}

const FILE_EXTENSIONS: [&str; 1] = ["xml"];

impl Importer for ImportXML {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let all_files = import_corpus_graph_from_files(&mut update, input_path, &FILE_EXTENSIONS)?;
        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), all_files.len())?;
        all_files.into_iter().try_for_each(|(p, d)| {
            self.import_document(&step_id, p.as_path(), d, &mut update, &progress)
        })?;
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

const GENERIC_NS: &str = "xml";

impl ImportXML {
    fn import_document(
        &self,
        step_id: &StepID,
        path: &Path,
        doc_node_name: String,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
    ) -> Result<()> {
        // parsing
        let f = File::open(path)?;
        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        // stacks and lists
        let default_key = "".to_string();
        let mut node_counts = BTreeMap::default();
        node_counts.insert(default_key.to_string(), 0_usize);
        let mut node_stack: Vec<(String, String)> = Vec::new();
        // this stack collects the text values not for use in tokens, but for use in annotation values
        // for the tag-nodes with their name-specific annotation
        let mut value_stack = Vec::default();
        loop {
            let xml_event = reader.next().map_err(|_| AnnattoError::Import {
                reason: "Error parsing xml.".to_string(),
                importer: step_id.module_name.clone(),
                path: path.to_path_buf(),
            })?;
            match xml_event {
                xml::reader::XmlEvent::StartElement {
                    name, attributes, ..
                } => {
                    let name_str = name.local_name.to_string();
                    node_counts
                        .entry(name_str.to_string())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                    let node_name = format!("{doc_node_name}#{}{}", name, node_counts[&name_str]);
                    add_node(update, &doc_node_name, &node_name, None)?;
                    if let Some((dom_node_name, _)) = node_stack.last() {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: dom_node_name.to_string(),
                            target_node: node_name.to_string(),
                            layer: GENERIC_NS.to_string(),
                            component_type: AnnotationComponentType::Dominance.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    // It is important that before running for a text attribute below, the latest node is pushed onto the node stack
                    // this way the node directly dominates its text which it is required to do
                    node_stack.push((node_name.to_string(), name.to_string()));
                    let text_attr_name = self.text_from_attribute.get(&name_str);
                    for attr in attributes {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: GENERIC_NS.to_string(),
                            anno_name: attr.name.to_string(),
                            anno_value: attr.value.to_string(),
                        })?;
                        if let Some(target_name) = text_attr_name
                            && &attr.name.to_string() == target_name
                        {
                            let token_text_from_attr = &attr.value;
                            for t in token_text_from_attr.chars() {
                                build_token(
                                    update,
                                    &doc_node_name,
                                    t.to_string(),
                                    &mut node_counts,
                                    &node_stack,
                                )?;
                            }
                        }
                    }
                    value_stack.push(String::new());
                }
                xml::reader::XmlEvent::EndElement { name } => {
                    if let Some((node_name, pop_node_type)) = node_stack.last()
                        && &name.to_string() == pop_node_type
                    {
                        if let Some(last_string) = value_stack.pop() {
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_name.to_string(),
                                anno_ns: GENERIC_NS.to_string(),
                                anno_name: name.local_name.to_string(),
                                anno_value: last_string.to_string(),
                            })?;
                            if let Some(new_last) = value_stack.last_mut() {
                                new_last.push_str(&last_string);
                            }
                        }
                        if !self.closing_default.is_empty() {
                            for t in self.closing_default.chars() {
                                build_token(
                                    update,
                                    &doc_node_name,
                                    t.to_string(),
                                    &mut node_counts,
                                    &node_stack,
                                )?;
                            }
                        }
                        node_stack.pop();
                    }
                }
                xml::reader::XmlEvent::Characters(chars) | xml::reader::XmlEvent::CData(chars) => {
                    for token in chars.chars() {
                        build_token(
                            update,
                            &doc_node_name,
                            token.to_string(),
                            &mut node_counts,
                            &node_stack,
                        )?;
                        if let Some(last_string) = value_stack.last_mut() {
                            last_string.push(token);
                        }
                    }
                }
                xml::reader::XmlEvent::EndDocument => {
                    progress.worked(1)?;
                    return Ok(());
                }
                _ => {}
            }
        }
    }
}

fn token_name(doc_node_name: &str, n: usize) -> String {
    format!("{doc_node_name}#{n}")
}

fn build_token(
    update: &mut GraphUpdate,
    doc_node_name: &str,
    token_value: String,
    node_counts: &mut BTreeMap<String, usize>,
    node_stack: &[(String, String)],
) -> Result<()> {
    node_counts
        .entry("".to_string())
        .and_modify(|e| *e += 1)
        .or_insert(1);
    let node_name = token_name(doc_node_name, node_counts[""]);
    let order_data = if node_counts[""] > 1 {
        Some((token_name(doc_node_name, node_counts[""] - 1), ""))
    } else {
        None
    };
    add_node(update, doc_node_name, &node_name, order_data)?;
    integrate_token(update, &node_name, token_value, node_stack)?;
    Ok(())
}

fn add_node(
    update: &mut GraphUpdate,
    doc_node_name: &str,
    node_name: &str,
    order_data: Option<(String, &str)>,
) -> Result<()> {
    update.add_event(UpdateEvent::AddNode {
        node_name: node_name.to_string(),
        node_type: "node".to_string(),
    })?;
    update.add_event(UpdateEvent::AddNodeLabel {
        node_name: node_name.to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "layer".to_string(),
        anno_value: "default_layer".to_string(),
    })?;
    update.add_event(UpdateEvent::AddEdge {
        source_node: node_name.to_string(),
        target_node: doc_node_name.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    if let Some((prev_node_name, order_name)) = order_data {
        update.add_event(UpdateEvent::AddEdge {
            source_node: prev_node_name,
            target_node: node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: order_name.to_string(),
        })?;
    }
    Ok(())
}

fn integrate_token(
    update: &mut GraphUpdate,
    token_name: &str,
    value: String,
    node_stack: &[(String, String)],
) -> Result<()> {
    update.add_event(UpdateEvent::AddNodeLabel {
        node_name: token_name.to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok".to_string(),
        anno_value: value,
    })?;
    for (cov_node_name, _) in node_stack {
        update.add_event(UpdateEvent::AddEdge {
            source_node: cov_node_name.to_string(),
            target_node: token_name.to_string(),
            layer: GENERIC_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
    }
    if let Some((dom_node, _)) = node_stack.last() {
        update.add_event(UpdateEvent::AddEdge {
            source_node: dom_node.to_string(),
            target_node: token_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "".to_string(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
