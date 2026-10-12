use std::{collections::BTreeMap, fs::File, path::Path};

use facet::Facet;
use graphannis::model::{AnnotationComponent, AnnotationComponentType};
use graphannis_core::graph::ANNIS_NS;
use lazy_static::lazy_static;
use serde::Serialize;
use serde_derive::Deserialize;
use xml::{EventReader, ParserConfig};

use crate::{
    StepID,
    error::{AnnattoError, Result},
    importer::DefaultImportConfiguration,
    progress::ProgressReporter,
    util::graphupdate::UpdateWriter,
};

use super::Importer;

/// Generic importer for XML files.
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
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
        mut update: UpdateWriter,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let all_files = update.write_corpus_graph_from_file_tree(input_path)?;
        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), all_files.len())?;
        all_files.into_iter().try_for_each(|(p, d)| {
            self.import_document(&step_id, p.as_path(), d, &mut update, &progress)
        })?;
        Ok(update.into())
    }
}

lazy_static! {
    static ref DEFAULT_DOMINANCE_COMPONENT: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Dominance,
        GENERIC_NS.to_string(),
        "".to_string()
    );
    static ref DEFAULT_COVERAGE_COMPONENT: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Coverage,
        GENERIC_NS.to_string(),
        "".to_string()
    );
}

impl DefaultImportConfiguration for ImportXML {
    fn default_file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }

    fn preset_default_namespace(&self) -> Option<&str> {
        Some(GENERIC_NS)
    }

    fn preset_default_dominance(&self) -> Option<&AnnotationComponent> {
        Some(&DEFAULT_DOMINANCE_COMPONENT)
    }

    fn preset_default_coverage(&self) -> Option<&AnnotationComponent> {
        Some(&DEFAULT_COVERAGE_COMPONENT)
    }
}

const GENERIC_NS: &str = "xml";

impl ImportXML {
    fn import_document(
        &self,
        step_id: &StepID,
        path: &Path,
        doc_node_name: String,
        update: &mut UpdateWriter,
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
                        update.dominance(dom_node_name.to_string(), node_name.to_string())?;
                    }
                    // It is important that before running for a text attribute below, the latest node is pushed onto the node stack
                    // this way the node directly dominates its text which it is required to do
                    node_stack.push((node_name.to_string(), name.to_string()));
                    let text_attr_name = self.text_from_attribute.get(&name_str);
                    for attr in attributes {
                        update.node_label(
                            node_name.to_string(),
                            update.config.default_namespace().to_string(),
                            attr.name.to_string(),
                            attr.value.to_string(),
                        )?;
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
                            update.node_label(
                                node_name.to_string(),
                                update.config.default_namespace().to_string(),
                                name.local_name.to_string(),
                                last_string.to_string(),
                            )?;
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
    update: &mut UpdateWriter,
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
    update: &mut UpdateWriter,
    doc_node_name: &str,
    node_name: &str,
    order_data: Option<(String, &str)>,
) -> Result<()> {
    update.labeled_node(
        node_name.to_string(),
        "node".to_string(),
        ANNIS_NS.to_string(),
        "layer".to_string(),
        "default_layer".to_string(),
    )?;
    update.part_of(node_name.to_string(), doc_node_name.to_string())?;
    if let Some((prev_node_name, order_name)) = order_data {
        update.edge_by_parts(
            prev_node_name,
            node_name.to_string(),
            update.config.default_ordering().layer.to_string(),
            update.config.default_ordering().get_type(),
            order_name.to_string(),
        )?;
    }
    Ok(())
}

fn integrate_token(
    update: &mut UpdateWriter,
    token_name: &str,
    value: String,
    node_stack: &[(String, String)],
) -> Result<()> {
    update.node_label(
        token_name.to_string(),
        ANNIS_NS.to_string(),
        "tok".to_string(),
        value,
    )?;
    for (cov_node_name, _) in node_stack {
        update.coverage(cov_node_name.to_string(), token_name.to_string())?;
    }
    if let Some((dom_node, _)) = node_stack.last() {
        update.dominance(dom_node.to_string(), token_name.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
