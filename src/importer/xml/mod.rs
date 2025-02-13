use std::{collections::BTreeMap, fs::File, path::Path};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use xml::{EventReader, ParserConfig};

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
    StepID,
};
use documented::{Documented, DocumentedFields};

use super::Importer;

/// Generic importer for XML files.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ImportXML {}

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
        let mut text_stack = Vec::default();
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
                    let name_str = name.to_string();
                    node_counts
                        .entry(name_str.to_string())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                    let node_name = format!("{doc_node_name}#{}{}", name, node_counts[&name_str]);
                    add_node(update, &doc_node_name, &node_name, None)?;
                    for attr in attributes {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: GENERIC_NS.to_string(),
                            anno_name: attr.name.to_string(),
                            anno_value: attr.value.to_string(),
                        })?;
                    }
                    if let Some((dom_node_name, _)) = node_stack.last() {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: dom_node_name.to_string(),
                            target_node: node_name.to_string(),
                            layer: GENERIC_NS.to_string(),
                            component_type: AnnotationComponentType::Dominance.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    node_stack.push((node_name, name.to_string()));
                    text_stack.push(String::new());
                }
                xml::reader::XmlEvent::EndElement { name } => {
                    if let Some((node_name, pop_node_type)) = node_stack.last() {
                        if &name.to_string() == pop_node_type {
                            if let Some(last_string) = text_stack.pop() {
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: GENERIC_NS.to_string(),
                                    anno_name: name.local_name.to_string(),
                                    anno_value: last_string.to_string(),
                                })?;
                                if let Some(new_last) = text_stack.last_mut() {
                                    new_last.push_str(&last_string);
                                }
                            }
                            node_stack.pop();
                        }
                    }
                }
                xml::reader::XmlEvent::Characters(chars) | xml::reader::XmlEvent::CData(chars) => {
                    for token in chars.chars() {
                        node_counts
                            .entry(default_key.to_string())
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                        let node_name = format!("{doc_node_name}#{}", node_counts[""]);
                        let order_data = if node_counts[""] > 1 {
                            Some((format!("{doc_node_name}#{}", node_counts[""] - 1), ""))
                        } else {
                            None
                        };
                        add_node(update, &doc_node_name, &node_name, order_data)?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "tok".to_string(),
                            anno_value: token.to_string(),
                        })?;
                        for (cov_node_name, _) in &node_stack {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: cov_node_name.to_string(),
                                target_node: node_name.to_string(),
                                layer: GENERIC_NS.to_string(),
                                component_type: AnnotationComponentType::Coverage.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                        if let Some((dom_node, _)) = node_stack.last() {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: dom_node.to_string(),
                                target_node: node_name.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Dominance.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                        if let Some(last_string) = text_stack.last_mut() {
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

#[cfg(test)]
mod tests;
