mod model;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use crate::{importer::Importer, progress::ProgressReporter};

#[derive(Clone, Default, Deserialize, Facet, PartialEq, Serialize)]
/// This importer reads ELAN files.
pub struct ImportELAN {
    /// The listed annotation names will be treated as segmentations (in the graphANNIS sense)
    /// and be equipped with an ordering `Ordering/default_ns/{tier_name}`. A segmentation in
    /// this sense would in other contexts be called a "tokenization". Sentence spans, on the
    /// other hand, are usually not segmentations in the graphANNIS sense, unless you strictly
    /// need them to be.
    ///
    /// If your annotation names contain spaces, replace these with "_".
    #[serde(default)]
    segmentations: BTreeSet<String>,
    /// Setting this to `true` suppresses the creation of time annotations. In the default case,
    /// these are created when the time unit in the ELAN file is milliseconds.
    #[serde(default)]
    skip_time: bool,
}

const DEFAULT_FILE_EXTENSIONS: [&str; 1] = ["eaf"];

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
            doc_node_name,
            segmentations: &self.segmentations,
            skip_time: self.skip_time,
        }
        .map(update)
    }
}

struct Timeline {
    node_sequence: Vec<String>,
    id_to_index: LinkedHashMap<String, usize>,
    id_to_time: BTreeMap<String, f64>,
    synonyms: BTreeMap<String, String>,
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

    /// There can be more than one timelot slot per time value.
    /// This is unified to a single id.
    fn unified_timeslot_id(&'a self, ts_id: &'a String) -> &'a String {
        self.synonyms.get(ts_id).unwrap_or(ts_id)
    }

    fn new(
        node_sequence: Vec<String>,
        id_to_index: LinkedHashMap<String, usize>,
        time_values: BTreeMap<String, f64>,
    ) -> Self {
        let mut synonyms = BTreeMap::default();
        let mut used_indices = BTreeMap::<usize, &String>::default();
        for (ts_id, index) in &id_to_index {
            if let Some(id) = used_indices.get(index).copied() {
                synonyms.insert(ts_id.to_string(), id.to_string());
                continue;
            }
            synonyms.insert(ts_id.to_string(), ts_id.to_string());
            used_indices.insert(*index, ts_id);
        }
        Timeline {
            node_sequence,
            id_to_index,
            id_to_time: time_values,
            synonyms,
        }
    }
}

struct DocumentScan<'a> {
    slot_nodes: BTreeMap<String, Vec<String>>,
    anno_layers: BTreeMap<String, Vec<&'a String>>,
    anno_intervals: BTreeMap<&'a String, (String, String)>,
    anno_values: BTreeMap<&'a String, &'a String>,
}

fn flatten_slots(slots: &BTreeMap<String, Vec<String>>) -> BTreeMap<String, Vec<String>> {
    fn recursive_inner(subslots: &[String], lookup: &BTreeMap<String, Vec<String>>) -> Vec<String> {
        let mut flat_slots = Vec::with_capacity(subslots.len() + 10); // some buffer to avoid re-allocation, 10 is kind of random at this point    
        for slot in subslots {
            flat_slots.push(slot.to_string());
            if let Some(extra_slots) = lookup.get(slot)
                && extra_slots.len() > 1
            {
                flat_slots.extend(recursive_inner(&extra_slots[1..], lookup));
            }
        }
        flat_slots
    }

    let mut flattened_slots = BTreeMap::default();
    for (slot, subslots) in slots {
        flattened_slots.insert(slot.to_string(), recursive_inner(subslots, slots));
    }
    flattened_slots
}

struct ELANMapper<'a> {
    data: model::AnnotationDocument,
    doc_node_name: &'a str,
    segmentations: &'a BTreeSet<String>,
    skip_time: bool,
}

impl<'a> ELANMapper<'a> {
    fn map(&mut self, update: &mut GraphUpdate) -> crate::error::Result<()> {
        let timeline = self.scan_timeline()?;
        self.data.sort_tiers();
        let scan = self.scan_tiers(&timeline)?;
        self.build(timeline, scan, update)?;
        Ok(())
    }

    fn scan_timeline(&self) -> crate::error::Result<Timeline> {
        let mut id_to_index = LinkedHashMap::default();
        let mut node_sequence = Vec::with_capacity(self.data.timeline().len());
        let mut time_to_node_name = BTreeMap::default();
        let mut id_to_time = BTreeMap::default();
        for time_slot in self.data.timeline() {
            let node_name = format!("{}#{}", self.doc_node_name, time_slot.time_slot_id);
            // Elan allows for having several time slots for the same time, so make sure that
            // for each time value, there is only one node
            if let Some(time_val) = &time_slot.time_value {
                if let model::TimeUnits::Milliseconds = self.data.time_units() {
                    id_to_time.insert(
                        time_slot.time_slot_id.to_string(),
                        (*time_val as f64) / 1000f64,
                    );
                }
                if let Some(existing_time_slot) = time_to_node_name.get(time_val)
                    && let Some(index) = id_to_index.get(existing_time_slot)
                {
                    id_to_index.insert(time_slot.time_slot_id.to_string(), *index);
                    continue;
                } else {
                    time_to_node_name.insert(*time_val, time_slot.time_slot_id.to_string());
                }
            }
            id_to_index.insert(time_slot.time_slot_id.to_string(), node_sequence.len());
            node_sequence.push(node_name.to_string());
        }
        Ok(Timeline::new(node_sequence, id_to_index, id_to_time))
    }

    fn scan_tiers(&'a self, timeline: &Timeline) -> crate::error::Result<DocumentScan<'a>> {
        let mut anno_id_to_interval = BTreeMap::<&String, (String, String)>::default();
        let mut anno_id_to_value = BTreeMap::default();
        let mut subslots = BTreeMap::<String, Vec<String>>::default();
        let mut layers = BTreeMap::default();
        for slot in timeline.id_to_index.keys() {
            subslots.insert(slot.to_string(), vec![format!("{slot}_0")]);
        }
        for tier in self.data.tiers() {
            let tier_id = tier.clean_id();
            let mut anno_ids = Vec::with_capacity(tier.annotations().len());
            let mut tier_iter = tier.annotations().into_iter().peekable();
            while let Some(annotation) = tier_iter.next() {
                match annotation {
                    model::Annotation::AlignableAnnotation {
                        annotation_value,
                        time_slot_ref1,
                        time_slot_ref2,
                        annotation_id,
                        ..
                    } => {
                        // normalization of timeslots only needs to happen here when timeslots are actually used
                        let time_slot_ref1 = timeline.unified_timeslot_id(time_slot_ref1);
                        let time_slot_ref2 = timeline.unified_timeslot_id(time_slot_ref2);
                        anno_id_to_value.insert(annotation_id, annotation_value);
                        anno_id_to_interval.insert(
                            annotation_id,
                            (time_slot_ref1.to_string(), time_slot_ref2.to_string()),
                        );
                        anno_ids.push(annotation_id);
                    }
                    model::Annotation::RefAnnotation {
                        annotation_value,
                        annotation_ref,
                        annotation_id,
                        ..
                    } => {
                        // are we entering a chain?
                        if let Some(model::Annotation::RefAnnotation {
                            previous_annotation: Some(_),
                            ..
                        }) = tier_iter.peek()
                        {
                            // branch: yes
                            // chains are sequences of annotations that all refer to the same parent
                            // annotation and thus require a split of a timeline node into multiple
                            // nodes.
                            // We KNOW the parent ref was already processed (tiers are sorted) and
                            // a time value can be recovered.
                            if let Some(interval) = anno_id_to_interval.get(annotation_ref).cloned()
                            {
                                let tail = {
                                    let mut remaining_members = Vec::default();
                                    while let Some(anno) = tier_iter.next_if(|nxt| {
                                        matches!(
                                            nxt,
                                            model::Annotation::RefAnnotation {
                                                previous_annotation: Some(_),
                                                ..
                                            }
                                        )
                                    }) {
                                        remaining_members.push(anno);
                                    }
                                    remaining_members
                                };
                                let (time_slot_ref1, time_slot_ref2) = interval;
                                {
                                    // handle first chain member
                                    anno_id_to_value.insert(annotation_id, annotation_value);
                                    anno_ids.push(annotation_id);
                                }
                                // move on: check tslot granularity for correct value
                                if let Some(mut real_slots) = subslots.remove(&time_slot_ref1) {
                                    let use_n_per_anno = if real_slots.len() == 1 {
                                        // new subslots need to be built (for the remainder of annotations)
                                        for i in 0..tail.len() {
                                            let slot_id = format!("{time_slot_ref1}_{}", i + 1);
                                            subslots.insert(
                                                slot_id.to_string(),
                                                vec![slot_id.to_string()],
                                            );
                                            real_slots.push(slot_id);
                                        }
                                        1
                                    } else if let Some(p) =
                                        real_slots.iter().position(|t| t == &time_slot_ref2)
                                    {
                                        if p != 1 && p != tail.len() + 1 {
                                            return Err(anyhow!("Granularity of subslots does not match annotations.").into());
                                        }
                                        if p == tail.len() + 1 {
                                            1
                                        } else {
                                            // p == 1
                                            // create extra subslots
                                            let mut new_slots = Vec::with_capacity(tail.len());
                                            for i in 0..tail.len() {
                                                let slot_id =
                                                    format!("{time_slot_ref1}_0_{}", i + 1);
                                                subslots.insert(
                                                    slot_id.to_string(),
                                                    vec![slot_id.to_string()],
                                                );
                                                new_slots.push(slot_id);
                                            }
                                            real_slots = [real_slots[0].to_string()]
                                                .into_iter()
                                                .chain(new_slots)
                                                .chain(real_slots.into_iter().dropping(1))
                                                .collect_vec();
                                            1
                                        }
                                    } else if real_slots.len() == tail.len() + 1 {
                                        1
                                    } else if real_slots.len() > tail.len() + 1
                                        && real_slots.len() % (tail.len() + 1) == 0
                                    {
                                        real_slots.len() / (tail.len() + 1)
                                    } else {
                                        return Err(anyhow!("Granularity mismatch for {time_slot_ref1} on tier {tier_id}").into());
                                    };
                                    {
                                        anno_id_to_interval.insert(
                                            annotation_id,
                                            (
                                                time_slot_ref1.to_string(),
                                                real_slots[use_n_per_anno].to_string(),
                                            ),
                                        );
                                    }
                                    let time_anno_tuples = real_slots
                                        .iter()
                                        .dropping(use_n_per_anno)
                                        .step_by(use_n_per_anno)
                                        .zip(
                                            real_slots
                                                .iter()
                                                .chain([&time_slot_ref2])
                                                .dropping(2 * use_n_per_anno) // make sure the last interval can be built
                                                .step_by(use_n_per_anno),
                                        )
                                        .zip(tail);
                                    for ((start_slot, end_slot_excl), anno) in time_anno_tuples {
                                        anno_id_to_value.insert(anno.id(), anno.value());
                                        anno_id_to_interval.insert(
                                            anno.id(),
                                            (start_slot.to_string(), end_slot_excl.to_string()),
                                        );
                                        anno_ids.push(anno.id());
                                    }
                                    subslots.insert(time_slot_ref1.to_string(), real_slots);
                                } else {
                                    return Err(
                                        anyhow!("Unknown time slot: {time_slot_ref1}").into()
                                    );
                                }
                            }
                        } else {
                            // branch: no
                            // at this point, as the tiers are sorted, all alignable annotations are known,
                            // and we can thus assume that the timeline slot the current annotation transitively
                            // refers to can be recovered.
                            anno_id_to_value.insert(annotation_id, annotation_value);
                            anno_ids.push(annotation_id);
                            if let Some(interval) = anno_id_to_interval.get(annotation_ref).cloned()
                            {
                                anno_id_to_interval.insert(annotation_id, interval);
                            } else {
                                return Err(anyhow!(
                                    "Unknown annotation reference: {}",
                                    annotation_ref
                                )
                                .into());
                            }
                        }
                    }
                }
            }
            layers.insert(tier_id, anno_ids);
        }
        Ok(DocumentScan {
            slot_nodes: flatten_slots(&subslots),
            anno_layers: layers,
            anno_intervals: anno_id_to_interval,
            anno_values: anno_id_to_value,
        })
    }

    fn build(
        &self,
        timeline: Timeline,
        scan: DocumentScan,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        // build time line
        let mut ts_id_to_node_name: LinkedHashMap<String, String> = LinkedHashMap::default();
        let mut used_timeslots = BTreeSet::default();
        let mut predecessor = None;
        for (ts_id, index) in &timeline.id_to_index {
            let ts_id = timeline.unified_timeslot_id(ts_id);
            if used_timeslots.contains(ts_id) {
                continue;
            }
            used_timeslots.insert(ts_id);
            predecessor = if let Some(first_node) = timeline.node_sequence.get(*index) {
                update.add_event(UpdateEvent::AddNode {
                    node_name: first_node.to_string(),
                    node_type: "node".to_string(),
                })?;
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: first_node.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: " ".to_string(),
                })?;
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: first_node.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "layer".to_string(),
                    anno_value: "default_layer".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: first_node.to_string(),
                    target_node: self.doc_node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                ts_id_to_node_name.insert(ts_id.to_string(), first_node.to_string());
                if let Some(slot_list) = scan.slot_nodes.get(ts_id)
                    && let Some(pseudonym) = slot_list.get(0)
                {
                    ts_id_to_node_name.insert(pseudonym.to_string(), first_node.to_string());
                }
                if let Some(prenode) = predecessor {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: prenode,
                        target_node: first_node.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                Some(first_node.to_string())
            } else {
                return Err(anyhow!("Unknown time slot: {ts_id}").into());
            };
            for subnode_id in scan
                .slot_nodes
                .get(ts_id)
                .ok_or(anyhow!("Unknown slot id: {ts_id}"))?
                .iter()
                .dropping(1)
            {
                let subnode_name = format!("{}#{subnode_id}", self.doc_node_name);
                update.add_event(UpdateEvent::AddNode {
                    node_name: subnode_name.to_string(),
                    node_type: "node".to_string(),
                })?;
                if let Some(prenode) = predecessor {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: prenode,
                        target_node: subnode_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                predecessor = Some(subnode_name.to_string());
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: subnode_name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: " ".to_string(),
                })?;
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: subnode_name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "layer".to_string(),
                    anno_value: "default_layer".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: subnode_name.to_string(),
                    target_node: self.doc_node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                ts_id_to_node_name.insert(subnode_id.to_string(), subnode_name);
            }
        }
        // build refreshed timeline struct
        let new_node_sequence = ts_id_to_node_name
            .values()
            .map(ToString::to_string)
            .collect_vec();
        let new_index_map = ts_id_to_node_name
            .into_iter()
            .enumerate()
            .map(|(i, (ts_id, _))| (ts_id, i))
            .collect();
        let full_timeline = Timeline {
            node_sequence: new_node_sequence,
            id_to_index: new_index_map,
            id_to_time: timeline.id_to_time,
            synonyms: BTreeMap::default(), // from now on there are only valid timeslot ids, so a mapping can pass through it's input
        };
        // map layers onto timeline
        for (anno_name, anno_ids) in scan.anno_layers {
            let build_ordering = self.segmentations.contains(&anno_name);
            let mut last_ordered_element = None;
            for anno_id in anno_ids {
                let (start, end_excl) = scan
                    .anno_intervals
                    .get(anno_id)
                    .ok_or(anyhow!("No interval specified for {anno_id}"))?;
                if let Some(targets) = full_timeline.slice(&start, &end_excl) {
                    let node_name = format!("{}#{anno_id}", self.doc_node_name);
                    update.add_event(UpdateEvent::AddNode {
                        node_name: node_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    if !self.skip_time
                        && let Some(start_time) = full_timeline.id_to_time.get(start)
                        && let Some(end_time) = full_timeline.id_to_time.get(end_excl)
                    {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: ANNIS_NS.to_string(),
                            anno_name: "time".to_string(),
                            anno_value: format!("{start_time}-{end_time}"),
                        })?;
                    }
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: node_name.to_string(),
                        target_node: self.doc_node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                    if let Some(anno_value) = scan.anno_values.get(anno_id) {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: "elan".to_string(),
                            anno_name: anno_name.to_string(),
                            anno_value: anno_value.to_string(),
                        })?
                    } else {
                        return Err(anyhow!("No value for annotation {anno_id}").into());
                    }
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "layer".to_string(),
                        anno_value: "default_layer".to_string(),
                    })?;
                    for target in targets {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: node_name.to_string(),
                            target_node: target.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    if build_ordering {
                        if let Some(previous_node) = last_ordered_element {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: previous_node,
                                target_node: node_name.to_string(),
                                layer: DEFAULT_NS.to_string(),
                                component_type: AnnotationComponentType::Ordering.to_string(),
                                component_name: anno_name.to_string(),
                            })?;
                        }
                        last_ordered_element = Some(node_name);
                    }
                } else {
                    return Err(anyhow!(
                        "Could not determine timeline targets for annotation {anno_id} in interval [{start}, {end_excl})."
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
