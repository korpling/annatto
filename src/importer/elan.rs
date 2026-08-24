mod model;

use std::{collections::BTreeMap, path::Path};

use anyhow::anyhow;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use linked_hash_map::LinkedHashMap;

use crate::{error::AnnattoError, importer::Importer, progress::ProgressReporter};

pub struct ImportELAN {}

const DEFAULT_FILE_EXTENSIONS: [&str; 2] = ["eaf", "xml"];

impl Importer for ImportELAN {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let named_paths = config.derive_corpus_graph(input_path, &mut update)?;
        let progress = ProgressReporter::new(tx, step_id, named_paths.len())?;
        named_paths
            .into_iter()
            .try_for_each(|(p, d)| self.import_document(&p, &d, &mut update, &progress))?;
        Ok(update)
    }

    fn default_file_extensions(&self) -> &[&str] {
        &DEFAULT_FILE_EXTENSIONS
    }
}

impl ImportELAN {
    fn import_document(
        &self,
        path: &std::path::Path,
        doc_node_name: &str,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
    ) -> crate::error::Result<()> {
        progress.worked(1)?;
        let file_contents = std::fs::read_to_string(path)?;
        let elan_data: model::AnnotationDocument =
            serde_xml_rs::from_str(&file_contents).map_err(|e| anyhow!(e.to_string()))?;
        ELANMapper {
            data: elan_data,
            doc_node_name: &doc_node_name,
        }
        .map(update)
    }
}

struct Timeline {
    node_sequence: Vec<String>,
    id_to_index: LinkedHashMap<String, usize>,
}

impl<'a> Timeline {
    fn slice(&'a self, start: &str, end_excl: &str) -> Option<&'a [String]> {
        if let Some(l) = self.id_to_index.get(start)
            && let Some(r) = self.id_to_index.get(end_excl)
        {
            let seq = &self.node_sequence[*l..*r];
            Some(seq)
        } else {
            None
        }
    }
}

struct ELANMapper<'a> {
    data: model::AnnotationDocument,
    doc_node_name: &'a str,
}

impl<'a> ELANMapper<'a> {
    fn map(&mut self, update: &mut GraphUpdate) -> crate::error::Result<()> {
        let timeline = self.map_timeline(update)?;
        self.map_tiers(timeline, update)?;
        Ok(())
    }

    fn map_timeline(&self, update: &mut GraphUpdate) -> crate::error::Result<Timeline> {
        let mut id_to_index = LinkedHashMap::default();
        let mut node_sequence = Vec::with_capacity(self.data.timeline().len());
        let mut predecessor: Option<String> = None;
        let mut previous_time = None;
        let mut time_to_node_name = BTreeMap::default();
        for time_slot in self.data.timeline() {
            let node_name = format!("{}#{}", self.doc_node_name, time_slot.time_slot_id);
            // Elan allows for having several time slots for the same time, so make sure that
            // for each time value, there is only one node
            if let Some(time_val) = &time_slot.time_value {
                if let Some(exisiting_time_slot) = time_to_node_name.get(time_val)
                    && let Some(index) = id_to_index.get(exisiting_time_slot)
                {
                    id_to_index.insert(time_slot.time_slot_id.to_string(), *index);
                    continue;
                } else {
                    time_to_node_name.insert(*time_val, time_slot.time_slot_id.to_string());
                }
            }
            id_to_index.insert(time_slot.time_slot_id.to_string(), node_sequence.len());
            node_sequence.push(node_name.to_string());
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
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: self.doc_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;

            if let Some(preceeding_node) = predecessor {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: preceeding_node.to_string(),
                    target_node: node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                if let Some(current_time_val) = time_slot.time_value {
                    if let Some(time_val) = previous_time {
                        if let model::TimeUnits::Milliseconds = self.data.time_units() {
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: preceeding_node,
                                anno_ns: ANNIS_NS.to_string(),
                                anno_name: "time".to_string(),
                                anno_value: format!(
                                    "{}-{}",
                                    time_val as f64 / 1000f64,
                                    current_time_val as f64 / 1000f64
                                ),
                            })?;
                        }
                    }
                    previous_time = Some(current_time_val);
                }
            }
            predecessor = Some(node_name);
        }
        if let Some(node_name) = predecessor
            && let Some(time) = previous_time
            && let model::TimeUnits::Milliseconds = self.data.time_units()
        {
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name,
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "time".to_string(),
                anno_value: format!("{}-", time as f64 / 1000f64),
            })?;
        }
        Ok(Timeline {
            node_sequence,
            id_to_index,
        })
    }

    fn map_tiers(
        &mut self,
        timeline: Timeline,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        let mut tier_data = BTreeMap::default();
        // map alignment tiers
        for tier in self.data.tiers() {
            let tier_id = tier.id().replace(" ", "_");
            dbg!(&tier_id);
            for anno in tier.annotations() {
                match anno {
                    model::Annotation::AlignableAnnotation {
                        annotation_value,
                        time_slot_ref1,
                        time_slot_ref2,
                        annotation_id,
                        ..
                    } => {
                        let targets = timeline
                            .slice(time_slot_ref1, time_slot_ref2)
                            .ok_or(anyhow!("Undefined timeline items."))?;
                        tier_data.insert(annotation_id, targets);
                        let annotation_node_name =
                            format!("{}#{}_{}", self.doc_node_name, tier_id, annotation_id);
                        update.add_event(UpdateEvent::AddNode {
                            node_name: annotation_node_name.to_string(),
                            node_type: "node".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: annotation_node_name.to_string(),
                            target_node: self.doc_node_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: annotation_node_name.to_string(),
                            anno_ns: DEFAULT_NS.to_string(),
                            anno_name: tier_id.to_string(),
                            anno_value: annotation_value.to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: annotation_node_name.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "layer".to_string(),
                            anno_value: "default_layer".to_string(),
                        })?;
                        for target in targets {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: annotation_node_name.to_string(),
                                target_node: target.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Coverage.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                    }
                    model::Annotation::RefAnnotation {
                        annotation_value,
                        annotation_ref,
                        annotation_id,
                        ..
                    } => {
                        if let Some(targets) = tier_data.remove(annotation_ref) {
                            let annotation_node_name =
                                format!("{}#{}_{}", self.doc_node_name, tier_id, annotation_id);
                            update.add_event(UpdateEvent::AddNode {
                                node_name: annotation_node_name.to_string(),
                                node_type: "node".to_string(),
                            })?;
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: annotation_node_name.to_string(),
                                target_node: self.doc_node_name.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::PartOf.to_string(),
                                component_name: "".to_string(),
                            })?;
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: annotation_node_name.to_string(),
                                anno_ns: "".to_string(),
                                anno_name: tier_id.to_string(),
                                anno_value: annotation_value.to_string(),
                            })?;
                            for target in targets {
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: annotation_node_name.to_string(),
                                    target_node: target.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::Coverage.to_string(),
                                    component_name: "".to_string(),
                                })?;
                            }
                            tier_data.insert(annotation_ref, targets);
                            tier_data.insert(annotation_id, targets);
                        } else {
                            return Err(AnnattoError::Import {
                                reason: format!(
                                    "Annotation reference `{annotation_ref}` is unknown for annotation `{annotation_id}` on tier `{}`",
                                    tier.id()
                                ),
                                importer: "elan".to_string(),
                                path: Path::new(self.doc_node_name).to_path_buf(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
