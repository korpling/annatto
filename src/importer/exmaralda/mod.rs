use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use xml::{attribute::OwnedAttribute, reader::XmlEvent, EventReader, ParserConfig};

use crate::{
    util::{graphupdate::map_audio_source, insert_corpus_nodes_from_path},
    workflow::StatusMessage,
    Module,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_exmaralda";

#[derive(Default)]
pub struct ImportEXMARaLDA {}

impl Module for ImportEXMARaLDA {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for ImportEXMARaLDA {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let path_pattern_path = input_path.join("**").join("*.exb");
        let path_pattern = path_pattern_path.to_str().unwrap();
        for file_path_r in glob::glob(path_pattern)? {
            let file_path = file_path_r?;
            self.import_document(input_path, &file_path, &mut update, &tx)?;
        }
        Ok(update)
    }
}

fn attr_vec_to_map(attributes: &[OwnedAttribute]) -> BTreeMap<String, String> {
    attributes
        .iter()
        .map(|attr| (attr.name.to_string(), attr.value.to_string()))
        .collect::<BTreeMap<String, String>>()
}

impl ImportEXMARaLDA {
    fn import_document(
        &self,
        corpus_path: &std::path::Path,
        document_path: &std::path::Path,
        update: &mut GraphUpdate,
        tx: &Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // buffers
        let mut doc_node_name = String::new();
        let mut char_buf = String::new();
        let mut timeline = BTreeMap::new();
        let mut ordered_tl_nodes: Vec<String> = Vec::new();
        let mut speaker_map = BTreeMap::new();
        let mut parent_map: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut already_defined: BTreeSet<String> = BTreeSet::new();
        let mut named_orderings: BTreeMap<String, Vec<(OrderedFloat<f64>, String)>> =
            BTreeMap::new();
        let mut time_to_tli_attrs: BTreeMap<OrderedFloat<f64>, Vec<String>> = BTreeMap::new();
        // reader
        let f = File::open(document_path)?;
        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        loop {
            match reader.next() {
                Ok(XmlEvent::EndDocument) => break,
                Ok(XmlEvent::StartDocument { .. }) => {
                    doc_node_name =
                        insert_corpus_nodes_from_path(update, corpus_path, document_path)?;
                }
                Ok(XmlEvent::Characters(value)) => char_buf.push_str(value.as_str()),
                Ok(XmlEvent::StartElement {
                    name, attributes, ..
                }) => {
                    parent_map.insert(name.to_string(), attr_vec_to_map(&attributes));
                    match name.to_string().as_str() {
                        "referenced-file" => {
                            if let Some(file_url) = attr_vec_to_map(&attributes).get("url") {
                                if let Some(parent_path) = document_path.parent() {
                                    let audio_path = parent_path.join(file_url);
                                    if audio_path.exists() {
                                        map_audio_source(
                                            update,
                                            audio_path.as_path(),
                                            corpus_path.to_str().unwrap(),
                                            &doc_node_name,
                                        )?;
                                    } else if let Some(sender) = tx {
                                        let msg = format!("Linked file {} could not be found to be linked in document {}", audio_path.as_path().to_string_lossy(), &doc_node_name);
                                        sender.send(StatusMessage::Warning(msg))?;
                                    }
                                };
                            }
                        }
                        "tli" => {
                            let attr_map = attr_vec_to_map(&attributes);
                            let time = attr_map["time"].parse::<OrderedFloat<f64>>()?;
                            time_to_tli_attrs
                                .entry(time)
                                .or_insert_with(Vec::default)
                                .push(attr_map["id"].to_string());
                        }
                        "event" | "abbreviation" => char_buf.clear(),
                        _ => {}
                    }
                }
                Ok(XmlEvent::EndElement { name }) => {
                    match name.to_string().as_str() {
                        "abbreviation" => {
                            // write speaker name to speaker table
                            let speaker_id = parent_map.get("speaker").unwrap()["id"].to_string();
                            let speaker_name = char_buf.to_string();
                            speaker_map.insert(speaker_id, speaker_name);
                        }
                        "common-timeline" => {
                            // build empty toks
                            for (time_value, tli_ids) in
                                time_to_tli_attrs.iter().sorted_by(|e0, e1| e0.0.cmp(e1.0))
                            {
                                let tli_id_suffix = tli_ids.join("_");
                                let node_name = format!("{}#{}", &doc_node_name, tli_id_suffix);
                                update.add_event(UpdateEvent::AddNode {
                                    node_name: node_name.to_string(),
                                    node_type: "node".to_string(),
                                })?;
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "tok".to_string(),
                                    anno_value: " ".to_string(),
                                })?;
                                for tli_id in tli_ids {
                                    timeline.insert(
                                        tli_id.to_string(),
                                        (*time_value, node_name.to_string()),
                                    );
                                }
                            }
                            // order timeline elements / empty toks
                            ordered_tl_nodes.extend(
                                timeline
                                    .iter()
                                    .sorted_by(|a, b| a.1 .0.cmp(&b.1 .0))
                                    .map(|t| t.0.to_string())
                                    .collect_vec(),
                            );
                            for i in 1..ordered_tl_nodes.len() {
                                let source = &timeline
                                    .get(ordered_tl_nodes.get(i - 1).unwrap())
                                    .unwrap()
                                    .1;
                                let target =
                                    &timeline.get(ordered_tl_nodes.get(i).unwrap()).unwrap().1;
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: source.to_string(),
                                    target_node: target.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::Ordering.to_string(),
                                    component_name: "".to_string(),
                                })?;
                            }
                        }
                        "event" => {
                            let text = char_buf.to_string();
                            let tier_info = parent_map.get("tier").unwrap();
                            let speaker_id = tier_info.get("speaker").unwrap();
                            let speaker_name = speaker_map.get(speaker_id).unwrap();
                            let anno_name = tier_info.get("category").unwrap();
                            let tier_type = if let Some(tpe) = tier_info.get("type") {
                                tpe
                            } else {
                                if let Some(sender) = tx {
                                    let msg = format!(
                                        "Could not determine tier type for {}::{}. Tier will be skipped.",
                                        &speaker_id, &anno_name
                                    );
                                    sender.send(StatusMessage::Warning(msg))?;
                                }
                                continue;
                            };
                            let event_info = parent_map.get("event").unwrap();
                            let start_id = if let Some(id) = event_info.get("start") {
                                id
                            } else {
                                if let Some(sender) = tx {
                                    let msg = format!(
                                            "Could not determine start id of currently processed event `{}`. Event will be skipped.",
                                            text
                                        );
                                    sender.send(StatusMessage::Warning(msg))?;
                                }
                                continue;
                            };
                            let end_id = if let Some(id) = event_info.get("end") {
                                id
                            } else {
                                if let Some(sender) = tx {
                                    let msg = format!(
                                            "Could not determine end id of currently processed event `{}`. Event will be skipped.",
                                            text
                                        );
                                    sender.send(StatusMessage::Warning(msg))?;
                                }
                                continue;
                            };
                            let start_i =
                                ordered_tl_nodes.iter().position(|e| e == start_id).unwrap();
                            let end_i = ordered_tl_nodes.iter().position(|e| e == end_id).unwrap();
                            let overlapped = &ordered_tl_nodes[start_i..end_i];
                            if overlapped.is_empty() {
                                if let Some(sender) = tx {
                                    let msg = format!("Event {}::{}:{}-{} does not cover any tokens and will be skipped.", &speaker_id, &anno_name, &start_id, &end_id);
                                    sender.send(StatusMessage::Warning(msg))?;
                                }
                                continue;
                            }
                            let node_name = format!(
                                "{}#{}_{}_{}-{}",
                                doc_node_name, tier_type, speaker_id, start_id, end_id
                            ); // this is not a unique id as not intended to be
                            let start_time = if let Some((t, _)) =
                                timeline.get(overlapped.first().unwrap())
                            {
                                t
                            } else {
                                if let Some(sender) = tx {
                                    let msg = format!(
                                            "Could not determine start time of event {}::{}:{}-{}. Event will be skipped.",
                                            &speaker_id, &anno_name, &start_id, &end_id
                                        );
                                    sender.send(StatusMessage::Warning(msg))?;
                                }
                                continue;
                            };
                            if !already_defined.contains(&node_name) {
                                update.add_event(UpdateEvent::AddNode {
                                    node_name: node_name.to_string(),
                                    node_type: "node".to_string(),
                                })?;
                                // coverage
                                for overlapped_id in overlapped {
                                    let (_, target_id) = timeline.get(overlapped_id).unwrap();
                                    update.add_event(UpdateEvent::AddEdge {
                                        source_node: node_name.to_string(),
                                        target_node: target_id.to_string(),
                                        layer: ANNIS_NS.to_string(),
                                        component_type: AnnotationComponentType::Coverage
                                            .to_string(),
                                        component_name: "".to_string(),
                                    })?;
                                }
                                let (end_time, _) =
                                    timeline.get(overlapped.last().unwrap()).unwrap();
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "time".to_string(),
                                    anno_value: format!("{}-{}", start_time, end_time),
                                })?;
                                already_defined.insert(node_name.to_string());
                            }
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_name.to_string(),
                                anno_ns: ANNIS_NS.to_string(),
                                anno_name: "layer".to_string(),
                                anno_value: speaker_name.to_string(),
                            })?;
                            if tier_type.as_str() == "t" {
                                // tokenization
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "tok".to_string(),
                                    anno_value: text.to_string(),
                                })?;
                                // order nodes
                                if !named_orderings.contains_key(anno_name) {
                                    named_orderings.insert(anno_name.to_string(), Vec::new());
                                }
                                named_orderings
                                    .get_mut(anno_name)
                                    .unwrap()
                                    .push((*start_time, node_name.to_string()));
                            }
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_name.to_string(),
                                anno_ns: speaker_name.to_string(),
                                anno_name: anno_name.to_string(),
                                anno_value: text.to_string(),
                            })?;
                        }
                        _ => {}
                    }
                    parent_map.remove(&name.to_string());
                }
                Err(_) => break,
                _ => continue,
            }
        }
        // build order relations
        for (name, node_name_vec) in named_orderings {
            let mut prev = None;
            for (_, node_name) in node_name_vec
                .into_iter()
                .sorted_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
            {
                if let Some(source) = prev {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: source,
                        target_node: node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: name.to_string(),
                    })?;
                }
                prev = Some(node_name);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
