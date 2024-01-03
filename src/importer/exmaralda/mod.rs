//! Import [EXMARaLDA partition editor](https://exmaralda.org/en/partitur-editor-en/)
//! (`.exb`) files.
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::File,
};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde_derive::Deserialize;
use xml::{attribute::OwnedAttribute, reader::XmlEvent, EventReader, ParserConfig};

use crate::{
    error::AnnattoError,
    progress::ProgressReporter,
    util::graphupdate::{import_corpus_graph_from_files, map_audio_source},
    workflow::StatusMessage,
    Module, StepID,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_exmaralda";

#[derive(Default, Deserialize)]
#[serde(default)]
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
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let all_files = import_corpus_graph_from_files(&mut update, input_path, &["exb", "xml"])?;
        let progress = ProgressReporter::new(tx.clone(), step_id, all_files.len())?;
        let document_status: Result<Vec<()>, AnnattoError> = all_files
            .into_iter()
            .map(|(fp, doc_node_name)| {
                self.import_document(&doc_node_name, fp.as_path(), &mut update, &progress, &tx)
            })
            .collect();
        // Check for any errors
        document_status?;
        Ok(update)
    }
}

fn attr_vec_to_map(attributes: &[OwnedAttribute]) -> BTreeMap<String, String> {
    attributes
        .iter()
        .map(|attr| (attr.name.to_string(), attr.value.to_string()))
        .collect::<BTreeMap<String, String>>()
}

pub const LANGUAGE_SEP: &str = ",";

impl ImportEXMARaLDA {
    fn import_document(
        &self,
        doc_node_name: &str,
        document_path: &std::path::Path,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
        tx: &Option<crate::workflow::StatusSender>,
    ) -> crate::error::Result<()> {
        // buffers
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
        let mut errors = Vec::default();
        loop {
            match reader.next() {
                Ok(XmlEvent::EndDocument) => break,
                Ok(XmlEvent::StartDocument { .. }) => {}
                Ok(XmlEvent::Characters(value)) => char_buf.push_str(value.as_str()),
                Ok(XmlEvent::StartElement {
                    name, attributes, ..
                }) => {
                    parent_map.insert(name.to_string(), attr_vec_to_map(&attributes));
                    let attr_map = parent_map.get(&name.to_string()).unwrap();
                    match name.to_string().as_str() {
                        "referenced-file" => {
                            if let Some(file_url) = attr_vec_to_map(&attributes).get("url") {
                                if let Some(parent_path) = document_path.parent() {
                                    let audio_path = parent_path.join(file_url);
                                    if audio_path.exists()
                                        && (audio_path.is_file() || audio_path.is_symlink())
                                    {
                                        if let Ok(rel_path) =
                                            audio_path.strip_prefix(env::current_dir()?)
                                        {
                                            map_audio_source(
                                                update,
                                                rel_path,
                                                doc_node_name.rsplit_once('/').unwrap().0,
                                                doc_node_name,
                                            )?;
                                        } else {
                                            progress.warn(format!("Could not map linked audio file in {}. Path is incorrect.", doc_node_name).as_str())?;
                                        }
                                    } else {
                                        let msg = format!("Linked file {} could not be found to be linked in document {}", audio_path.as_path().to_string_lossy(), &doc_node_name);
                                        progress.warn(&msg)?;
                                    }
                                };
                            }
                        }
                        "tli" => {
                            if let Some(time_value) = attr_map.get("time") {
                                let time =
                                    if let Ok(t_val) = time_value.parse::<OrderedFloat<f64>>() {
                                        t_val
                                    } else {
                                        let err = AnnattoError::Import {
                                            reason: "Failed to parse tli time value.".to_string(),
                                            importer: self.module_name().to_string(),
                                            path: document_path.to_path_buf(),
                                        };
                                        return Err(err);
                                    };
                                time_to_tli_attrs
                                    .entry(time)
                                    .or_default()
                                    .push(attr_map["id"].to_string());
                            } else {
                                let err = AnnattoError::Import {
                                    reason: "A timeline item does not have a time value."
                                        .to_string(),
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            }
                        }
                        "language" => {
                            if !char_buf.is_empty() {
                                char_buf.push_str(LANGUAGE_SEP);
                            }
                            if let Some(lang_value) = attr_map.get("lang") {
                                char_buf.push_str(lang_value);
                            }
                        }
                        "event" | "abbreviation" | "l1" | "l2" | "comment" | "languages-used"
                        | "ud-information" => char_buf.clear(),
                        _ => {}
                    }
                }
                Ok(XmlEvent::EndElement { name }) => {
                    let str_tag_name = name.to_string();
                    match str_tag_name.as_str() {
                        "abbreviation" | "l1" | "l2" | "comment" | "languages-used" => {
                            if let Some(parent) = parent_map.get("speaker") {
                                if !char_buf.trim().is_empty() {
                                    let speaker_id = parent["id"].to_string();
                                    if str_tag_name.as_str() == "abbreviation" {
                                        // write speaker name to speaker table
                                        let speaker_name = char_buf.to_string();
                                        speaker_map.insert(speaker_id.to_string(), speaker_name);
                                    }

                                    update.add_event(UpdateEvent::AddNodeLabel {
                                        // speaker table data as document meta annotation
                                        node_name: doc_node_name.to_string(),
                                        anno_ns: speaker_id.to_string(),
                                        anno_name: str_tag_name,
                                        anno_value: char_buf.to_string(),
                                    })?;
                                }
                            }
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
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: node_name.to_string(),
                                    target_node: doc_node_name.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::PartOf.to_string(),
                                    component_name: "".to_string(),
                                })?;
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
                            let speaker_id_opt = tier_info.get("speaker");
                            let speaker_id = if let Some(speaker_id_val) = speaker_id_opt {
                                speaker_id_val
                            } else {
                                let rs = "Undefined speaker (not defined in tier attributes).";
                                let err = AnnattoError::Import {
                                    reason: rs.to_string(),
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            };
                            let speaker_name_opt = speaker_map.get(speaker_id);
                            let speaker_name = if let Some(speaker_name_value) = speaker_name_opt {
                                speaker_name_value
                            } else {
                                let rs = format!(
                                    "Speaker `{speaker_id}` has not been defined in speaker-table."
                                );
                                let err = AnnattoError::Import {
                                    reason: rs,
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            };
                            let anno_name_opt = tier_info.get("category");
                            let anno_name = if let Some(anno_name_value) = anno_name_opt {
                                anno_name_value
                            } else {
                                let rs = "Tier encountered with undefined category attribute.";
                                let err = AnnattoError::Import {
                                    reason: rs.to_string(),
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            };
                            let tier_type = if let Some(tpe) = tier_info.get("type") {
                                tpe.as_str()
                            } else {
                                let msg = format!(
                                    "Could not determine tier type for {}::{}. Tier will be treated as annotation tier.",
                                    &speaker_id, &anno_name
                                );
                                progress.warn(&msg)?;
                                "a"
                            };
                            let event_info = parent_map.get("event").unwrap();
                            let start_id = if let Some(id) = event_info.get("start") {
                                id
                            } else {
                                // send "Failed", but continue to collect potential further errors in the file
                                let msg = format!(
                                    "Could not determine start id of currently processed event `{}`. Event will be skipped. Import will fail.",
                                    text
                                );
                                let err = AnnattoError::Import {
                                    reason: msg,
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                errors.push(err);

                                continue;
                            };
                            let end_id = if let Some(id) = event_info.get("end") {
                                id
                            } else {
                                // send "Failed", but continue to collect potential further errors in the file
                                let msg = format!(
                                            "Could not determine end id of currently processed event `{}`. Event will be skipped. Import will fail.",
                                            text
                                        );
                                let err = AnnattoError::Import {
                                    reason: msg,
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                errors.push(err);
                                continue;
                            };
                            let start_i = if let Some(i_val) =
                                ordered_tl_nodes.iter().position(|e| e == start_id)
                            {
                                i_val
                            } else {
                                let err = AnnattoError::Import {
                                    reason: format!("Unknown time line item: {start_id}"),
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            };
                            let end_i = if let Some(i_val) =
                                ordered_tl_nodes.iter().position(|e| e == end_id)
                            {
                                i_val
                            } else {
                                let err = AnnattoError::Import {
                                    reason: format!("Unknown time line item: {start_id}"),
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                };
                                return Err(err);
                            };
                            if start_i >= end_i {
                                let err_msg = format!("Start time is bigger than end time for ids: {start_id}--{end_id} ");
                                return Err(AnnattoError::Import {
                                    reason: err_msg,
                                    importer: self.module_name().to_string(),
                                    path: document_path.to_path_buf(),
                                });
                            }
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
                                let (end_time, _) = if let Some(t_name) =
                                    ordered_tl_nodes.get(end_i)
                                {
                                    // timeline and ordered tl nodes are directly dependent on each other, so we can safely unwrap
                                    timeline.get(t_name).unwrap()
                                } else {
                                    if let Some(sender) = tx {
                                        let msg = format!(
                                                "Could not determine end time of event {}::{}:{}-{}. Event will be skipped.",
                                                &speaker_id, &anno_name, &start_id, &end_id
                                            );
                                        sender.send(StatusMessage::Warning(msg))?;
                                    }
                                    continue;
                                };
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
                            if tier_type == "t" {
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
                        "ud-information" => {
                            if let Some(anno_name) = parent_map
                                .get("ud-information")
                                .unwrap()
                                .get("attribute-name")
                            {
                                let ns = if let Some(parent) = parent_map.get("speaker") {
                                    if let Some(speaker_id) = parent.get("id") {
                                        speaker_id.as_str()
                                    } else {
                                        ""
                                    }
                                } else {
                                    ""
                                };
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: doc_node_name.to_string(),
                                    anno_ns: ns.to_string(),
                                    anno_name: anno_name.to_string(),
                                    anno_value: char_buf.to_string(),
                                })?;
                            }
                        }
                        _ => {}
                    }
                    parent_map.remove(&name.to_string());
                }
                Err(_) => {
                    return Err(AnnattoError::Import {
                        reason: "Failed parsing EXMARaLDA XML.".to_string(),
                        importer: self.module_name().to_string(),
                        path: document_path.to_path_buf(),
                    })
                }
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

        if errors.is_empty() {
            Ok(())
        } else {
            Err(AnnattoError::ConversionFailed { errors })
        }
    }
}

#[cfg(test)]
mod tests;
