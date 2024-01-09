use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use serde_derive::Deserialize;
use xml::{EventReader, ParserConfig};

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
    Module,
};

use super::Importer;

#[derive(Deserialize)]
pub struct ImportXML {
    default_ordering: String,
    #[serde(default)]
    named_orderings: BTreeSet<String>,
    #[serde(default)]
    skip_names: BTreeSet<String>,
    #[serde(default)]
    use_ids: bool,
}

const MODULE_NAME: &str = "import_xml";

impl Module for ImportXML {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
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
        let progress = ProgressReporter::new(tx.clone(), step_id, all_files.len())?;
        all_files
            .into_iter()
            .try_for_each(|(p, d)| self.import_document(p.as_path(), d, &mut update, &progress))?;
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

const GENERIC_NS: &str = "generic";

impl ImportXML {
    fn import_document(
        &self,
        path: &Path,
        doc_node_id: String,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
    ) -> Result<()> {
        // stacks and lists
        let mut node_stack = vec![doc_node_id.to_string()];
        let mut ordering_ends_at: BTreeMap<String, String> = BTreeMap::new();
        let mut node_count = 1;
        let mut char_buffer = String::new();
        let mut ignore_chars = false;
        // parsing
        let f = File::open(path)?;
        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        loop {
            let xml_event = reader.next().map_err(|_| AnnattoError::Import {
                reason: "Error parsing xml.".to_string(),
                importer: MODULE_NAME.to_string(),
                path: path.to_path_buf(),
            })?;
            match xml_event {
                xml::reader::XmlEvent::StartElement {
                    name, attributes, ..
                } => {
                    let lookup = name.to_string();
                    if self.skip_names.contains(&lookup) {
                        ignore_chars = true;
                        continue;
                    }
                    ignore_chars = false;
                    let generic_id = format!("{}#n{node_count}", doc_node_id);
                    let node_id = if self.use_ids {
                        if let Some(attr) = attributes
                            .iter()
                            .filter(|a| a.name.to_string().as_str() == "id")
                            .last()
                        {
                            format!("{}#{}", doc_node_id, attr.value)
                        } else {
                            generic_id
                        }
                    } else {
                        generic_id
                    };
                    update.add_event(UpdateEvent::AddNode {
                        node_name: node_id.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    for attr in attributes {
                        let key = attr.name.to_string();
                        let value = attr.value.to_string();
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_id.to_string(),
                            anno_ns: name.to_string(),
                            anno_name: key.to_string(),
                            anno_value: value.trim().to_string(),
                        })?;
                    }
                    if self.default_ordering != lookup {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_id.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "layer".to_string(),
                            anno_value: GENERIC_NS.to_string(),
                        })?;
                    } else if node_stack.len() > 1 {
                        let empty_node = node_id.replace('#', "#t_");
                        update.add_event(UpdateEvent::AddNode {
                            node_name: empty_node.to_string(),
                            node_type: "node".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: node_id.to_string(),
                            target_node: empty_node.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                        for ascendent_node in &node_stack[1..] {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: ascendent_node.to_string(),
                                target_node: empty_node.to_string(),
                                layer: DEFAULT_NS.to_string(),
                                component_type: AnnotationComponentType::Coverage.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                        if let Some(source_node_id) = ordering_ends_at.get(&"".to_string()) {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: source_node_id.to_string(),
                                target_node: empty_node.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Ordering.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                        ordering_ends_at.insert("".to_string(), empty_node.to_string());
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: empty_node.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "layer".to_string(),
                            anno_value: "default_layer".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_id.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "layer".to_string(),
                            anno_value: GENERIC_NS.to_string(),
                        })?;
                    }
                    if self.named_orderings.contains(&lookup) {
                        if let Some(source_node_id) = ordering_ends_at.get(&lookup) {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: source_node_id.to_string(),
                                target_node: node_id.to_string(),
                                layer: DEFAULT_NS.to_string(),
                                component_type: AnnotationComponentType::Ordering.to_string(),
                                component_name: lookup.to_string(),
                            })?;
                        }
                        ordering_ends_at.insert(lookup.to_string(), node_id.to_string());
                        update.add_event(UpdateEvent::AddNodeLabel {
                            // might overwrite a previous layer annotation, but that is okay
                            node_name: node_id.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "layer".to_string(),
                            anno_value: name.to_string(),
                        })?;
                    }
                    node_stack.push(node_id.to_string());
                    node_count += 1;
                }
                xml::reader::XmlEvent::EndElement { name } => {
                    if self.skip_names.contains(&name.to_string()) {
                        continue;
                    }
                    if let Some(node_id) = node_stack.pop() {
                        let value = char_buffer.to_string();
                        char_buffer.clear();
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_id.to_string(),
                            anno_ns: GENERIC_NS.to_string(),
                            anno_name: name.to_string(),
                            anno_value: value.to_string(),
                        })?;
                        let lookup = name.to_string();
                        if self.default_ordering == lookup {
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_id.replace('#', "#t_"),
                                anno_ns: ANNIS_NS.to_string(),
                                anno_name: "tok".to_string(),
                                anno_value: value,
                            })?;
                        }
                    }
                }
                xml::reader::XmlEvent::Characters(chars) => {
                    if !ignore_chars {
                        char_buffer.push_str(chars.as_str());
                    }
                }
                xml::reader::XmlEvent::EndDocument => {
                    progress.worked(1)?;
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
