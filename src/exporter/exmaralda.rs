use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufWriter, Write},
    ops::Range,
    path::Path,
    sync::Arc,
};

use crate::{error::AnnattoError, util::Traverse, Module};
use graphannis::{
    graph::GraphStorage,
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
    types::AnnoKey,
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use quick_xml::{
    events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event},
    Writer,
};
use serde_derive::Deserialize;

use super::Exporter;

#[derive(Default, Deserialize)]
pub struct ExportExmaralda {}

const MODULE_NAME: &str = "export_exmaralda";

impl Module for ExportExmaralda {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Exporter for ExportExmaralda {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut node_buffer = NodeData::default();
        let mut edge_buffer = EdgeData::default();
        self.traverse(graph, &mut node_buffer, &mut edge_buffer)?;
        let (start_data, end_data, timeline_data, anno_data) = node_buffer;
        let ordering_data = edge_buffer;
        let doc_nodes = start_data.iter().map(|((d, _), _)| d).collect_vec();
        let node_annos = graph.get_node_annos();
        for doc_node_id in doc_nodes {
            let doc_name = node_annos
                .get_value_for_item(doc_node_id, &NODE_NAME_KEY)?
                .unwrap();
            let doc_path = output_path.join(format!("{}.exb", doc_name.to_string()));
            fs::create_dir_all(doc_path.as_path().parent().unwrap())?;
            let file = fs::File::create(doc_path.as_path())?;
            let mut writer = Writer::new_with_indent(BufWriter::new(file), b' ', 2);
            writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;
            writer.write_event(Event::Start(BytesStart::new("basic-transcription")))?;
            writer.write_event(Event::Start(BytesStart::new("head")))?;
            writer.write_event(Event::Start(BytesStart::new("meta-information")))?;
            writer.write_event(Event::Start(BytesStart::new("transcription-name")))?;
            writer.write_event(Event::End(BytesEnd::new("transcription-name")))?;
            let mut ref_file = BytesStart::new("referenced-file");
            ref_file.push_attribute(("url", ""));
            writer.write_event(Event::Start(ref_file))?;
            writer.write_event(Event::End(BytesEnd::new("referenced-file")))?;
            writer.write_event(Event::Start(BytesStart::new("ud-meta-information")))?;
            writer.write_event(Event::End(BytesEnd::new("ud-meta-information")))?;
            writer.write_event(Event::Start(BytesStart::new("comment")))?;
            writer.write_event(Event::End(BytesEnd::new("comment")))?;
            writer.write_event(Event::Start(BytesStart::new("transcription-convention")))?;
            writer.write_event(Event::End(BytesEnd::new("transcription-convention")))?;
            writer.write_event(Event::End(BytesEnd::new("meta-information")))?;
            writer.write_event(Event::Start(BytesStart::new("speakertable")))?;
            for speaker_name in anno_data
                .keys()
                .filter(|(d, (_, _))| d == doc_node_id)
                .map(|(_, (ns, _))| ns)
                .collect::<BTreeSet<&String>>()
            {
                if speaker_name == ANNIS_NS {
                    continue;
                }
                let mut speaker = BytesStart::new("speaker");
                speaker.push_attribute(("id", speaker_name.as_str()));
                writer.write_event(Event::Start(speaker))?;
                writer.write_event(Event::Start(BytesStart::new("abbreviation")))?;
                writer.write_event(Event::Text(BytesText::new(&speaker_name)))?;
                writer.write_event(Event::End(BytesEnd::new("abbreviation")))?;
                let mut sex = BytesStart::new("sex");
                let sex_val = if let Some(v) = node_annos.get_value_for_item(
                    doc_node_id,
                    &AnnoKey {
                        name: "sex".into(),
                        ns: speaker_name.into(),
                    },
                )? {
                    v.to_string()
                } else {
                    "u".to_string()
                };
                sex.push_attribute(("value", sex_val.as_str()));
                writer.write_event(Event::Start(sex))?;
                writer.write_event(Event::End(BytesEnd::new("sex")))?;
                for meta_key in ["languages-used", "l1", "l2", "comment"] {
                    // TODO `languages-used` is not mapped correctly, it requires children of type "language"
                    writer.write_event(Event::Start(BytesStart::new(meta_key)))?;
                    if let Some(v) = node_annos.get_value_for_item(
                        doc_node_id,
                        &AnnoKey {
                            name: meta_key.into(),
                            ns: speaker_name.into(),
                        },
                    )? {
                        writer.write_event(Event::Text(BytesText::new(&v)))?;
                    }
                    writer.write_event(Event::End(BytesEnd::new(meta_key)))?;
                }
                writer.write_event(Event::Start(BytesStart::new("ud-speaker-information")))?;
                writer.write_event(Event::End(BytesEnd::new("ud-speaker-information")))?;
                writer.write_event(Event::End(BytesEnd::new("speaker")))?;
            }
            writer.write_event(Event::End(BytesEnd::new("speakertable")))?;
            writer.write_event(Event::End(BytesEnd::new("head")))?;
            writer.write_event(Event::Start(BytesStart::new("basic-body")))?;
            writer.write_event(Event::Start(BytesStart::new("common-timeline")))?;
            // write timeline
            let timeline: BTreeMap<&(u64, String), &OrderedFloat<f32>> = timeline_data
                .iter()
                .filter(|((d, _), _)| d == doc_node_id)
                .collect();
            for ((_, tli_id), t) in &timeline {
                let mut tli = BytesStart::new("tli");
                tli.push_attribute(("id", tli_id.as_str()));
                tli.push_attribute(("time", t.to_string().as_str()));
                writer.write_event(Event::Start(tli))?;
                writer.write_event(Event::End(BytesEnd::new("tli")))?;
            }
            writer.write_event(Event::End(BytesEnd::new("common-timeline")))?;
            for (i, anno_key) in node_annos.annotation_keys()?.iter().enumerate() {
                if anno_key.ns == ANNIS_NS {
                    continue;
                }
                let lookup = (
                    *doc_node_id,
                    (anno_key.ns.to_string(), anno_key.name.to_string()),
                );
                if let Some(entries) = anno_data.get(&lookup) {
                    let sorted_entries = entries.iter().sorted_unstable_by(|a, b| {
                        let node_a = a.0;
                        let node_b = b.0;
                        let start_a = start_data.get(&(*doc_node_id, node_a)).unwrap();
                        let start_b = start_data.get(&(*doc_node_id, node_b)).unwrap();
                        let time_a = timeline.get(&(*doc_node_id, start_a.to_string())).unwrap();
                        let time_b = timeline.get(&(*doc_node_id, start_b.to_string())).unwrap();
                        time_a.total_cmp(time_b)
                    });
                    let tier_type = if let Some((node_id, _)) = entries.last() {
                        if ordering_data.contains(node_id) {
                            "t"
                        } else {
                            "a"
                        }
                    } else {
                        "a"
                    };
                    let mut tier = BytesStart::new("tier");
                    tier.push_attribute(("speaker", anno_key.ns.as_str()));
                    tier.push_attribute(("category", anno_key.name.as_str()));
                    tier.push_attribute(("type", tier_type));
                    tier.push_attribute(("id", format!("TIER{i}").as_str()));
                    writer.write_event(Event::Start(tier))?;
                    for (node_id, anno_value) in sorted_entries {
                        let start = start_data.get(&(*doc_node_id, *node_id)).unwrap();
                        let end = end_data.get(&(*doc_node_id, *node_id)).unwrap();
                        let mut event = BytesStart::new("event");
                        event.push_attribute(("start", start.as_str()));
                        event.push_attribute(("end", end.as_str()));
                        writer.write_event(Event::Start(event))?;
                        writer.write_event(Event::Text(BytesText::new(anno_value)))?;
                        writer.write_event(Event::End(BytesEnd::new("event")))?;
                    }
                    writer.write_event(Event::End(BytesEnd::new("tier")))?;
                }
            }
            writer.write_event(Event::End(BytesEnd::new("basic-body")))?;
            writer.write_event(Event::End(BytesEnd::new("basic-transcription")))?;
            writer.into_inner().flush()?;
        }
        Ok(())
    }
}

type NodeData = (TimeData, TimeData, TimelineData, AnnoData);
type TimeData = BTreeMap<(u64, u64), String>;
type AnnoData = BTreeMap<(u64, (String, String)), Vec<(u64, String)>>;
type OrderingData = BTreeSet<u64>; // node ids in this set are member of an ordering (relevant to determine tier type)
type TimelineData = BTreeMap<(u64, String), OrderedFloat<f32>>;
type EdgeData = OrderingData;

impl Traverse<NodeData, EdgeData> for ExportExmaralda {
    fn node(
        &self,
        _graph: &AnnotationGraph,
        _node: graphannis_core::types::NodeID,
        _component: &graphannis::model::AnnotationComponent,
        _buffer: &mut NodeData,
    ) -> crate::error::Result<()> {
        // this method will not be used, since it would only copy the data for this format
        // we have access to the same data later when we can use it more efficiently
        Ok(())
    }

    fn edge(
        &self,
        _graph: &AnnotationGraph,
        _edge: graphannis_core::types::Edge,
        _component: &graphannis::model::AnnotationComponent,
        _buffer: &mut EdgeData,
    ) -> crate::error::Result<()> {
        Ok(())
    }

    fn traverse(
        &self,
        graph: &AnnotationGraph,
        node_buffer: &mut NodeData,
        edge_buffer: &mut EdgeData,
    ) -> crate::error::Result<()> {
        let base_ordering_c = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        if let Some(storage) = graph.get_graphstorage(&base_ordering_c) {
            // get the root nodes of the base (timeline) ordering
            let base_ordering_root_nodes = storage
                .source_nodes()
                .into_iter()
                .filter(|r| match r {
                    Ok(n) => storage.get_ingoing_edges(*n).count() == 0,
                    Err(_) => false,
                })
                .map(|r| r.unwrap())
                .collect_vec();
            // map all roots to a document
            let part_of_c = AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            );
            let part_of_storage = graph.get_graphstorage(&part_of_c).unwrap(); // "PartOf/annis" is a required component
            let annis_doc = AnnoKey {
                ns: ANNIS_NS.into(),
                name: "doc".into(),
            };
            // note: the following produces multiple entries for the same document node
            // in case this exporter gets a graph that cannot be represented in EXMARaLDA
            // (e. g., multiple ordering roots in one document for the same named ordering)
            let document_with_ordering_root = base_ordering_root_nodes
                .iter()
                .filter_map(|n| {
                    let dfs =
                        CycleSafeDFS::new(part_of_storage.as_edgecontainer(), *n, 0, usize::MAX);
                    let mut ret = None;
                    for r in dfs {
                        let node = r.unwrap().node;
                        let terminate =
                            graph.get_node_annos().has_value_for_item(&node, &annis_doc);
                        if terminate.is_ok() && terminate.unwrap() {
                            ret = Some((node, *n));
                            break;
                        }
                    }
                    ret
                })
                .collect_vec();
            // walk the ordering for each document and gather the nodes
            let mut cache = BTreeMap::default();
            let vertical_components =
                graph.get_all_components(Some(AnnotationComponentType::Coverage), None); // consider extending by dominance
            let vertical_storages = vertical_components
                .iter()
                .filter_map(|c| graph.get_graphstorage(c))
                .collect_vec();
            let (start_at_tli, end_at_tli, tli2time, anno_data) = node_buffer;
            let ordering_data = edge_buffer;
            let mut processed_nodes = BTreeSet::default();
            let time_key = AnnoKey {
                ns: ANNIS_NS.into(),
                name: "time".into(),
            };
            for (doc_node, ordering_root) in document_with_ordering_root {
                let order_dfs =
                    CycleSafeDFS::new(storage.as_edgecontainer(), ordering_root, 0, usize::MAX);
                let mut time_values = BTreeSet::new();
                let mut max_dist = 0;
                for s in order_dfs {
                    let step = s?;
                    let timeline_token = step.node;
                    max_dist += 1;
                    let naive_time_value = OrderedFloat(step.distance as f32);
                    let tli_id = format!("T{}", step.distance);
                    tli2time.insert((doc_node, tli_id.to_string()), naive_time_value);
                    let next_tli_id = format!("T{}", 1 + step.distance);
                    start_at_tli.insert((doc_node, timeline_token), tli_id.to_string());
                    let mut covering_nodes = BTreeSet::default();
                    reachable_nodes(
                        timeline_token,
                        &vertical_storages,
                        &mut covering_nodes,
                        &mut cache,
                    )
                    .map_err(|_| AnnattoError::Export {
                        reason: "Could not determine reachable nodes".to_string(),
                        exporter: self.module_name().to_string(),
                        path: Path::new("./").to_path_buf(),
                    })?;
                    for n in &covering_nodes {
                        let k = (doc_node, *n);
                        if !start_at_tli.contains_key(&k) {
                            start_at_tli.insert(k, tli_id.to_string());
                        }
                        end_at_tli.insert(k, next_tli_id.to_string());
                        if !processed_nodes.contains(n) {
                            graph
                                .get_node_annos()
                                .get_annotations_for_item(n)
                                .map_err(|_| AnnattoError::Export {
                                    reason: "Could not gather annotations for a node.".to_string(),
                                    exporter: self.module_name().to_string(),
                                    path: Path::new("./").to_path_buf(),
                                })?
                                .into_iter()
                                .for_each(|a| {
                                    // collect annotations
                                    let anno_k =
                                        (doc_node, (a.key.ns.to_string(), a.key.name.to_string()));
                                    if !anno_data.contains_key(&anno_k) {
                                        anno_data.insert(anno_k, vec![(*n, a.val.to_string())]);
                                    } else {
                                        anno_data
                                            .get_mut(&anno_k)
                                            .unwrap()
                                            .push((*n, a.val.to_string()));
                                    }
                                });
                            // check for interval annotations
                            if let Ok(Some(interval)) =
                                graph.get_node_annos().get_value_for_item(n, &time_key)
                            {
                                if let Some(tpl) = interval.split_once("-") {
                                    for time_string in [tpl.0, tpl.1] {
                                        let time = time_string
                                            .parse::<OrderedFloat<f32>>()
                                            .map_err(|_| AnnattoError::Export {
                                                reason: format!("Failed to parse time value {time_string} of interval {interval}"),
                                                exporter: self.module_name().to_string(),
                                                path: Path::new("./").to_path_buf(),
                                            })?;
                                        time_values.insert(time);
                                    }
                                }
                            }
                            processed_nodes.insert(*n);
                        }
                    }
                }
                if time_values.len() > 0 {
                    for (i, t) in (0..max_dist + 2).zip(time_values.into_iter().sorted()) {
                        tli2time.insert((doc_node, format!("T{i}")), t);
                    }
                }
            }
            // collecting ordering data
            let mut storages = Vec::new();
            for ordering in graph.get_all_components(Some(AnnotationComponentType::Ordering), None)
            {
                if ordering.name.is_empty() {
                    continue;
                }
                if let Some(storage) = graph.get_graphstorage(&ordering) {
                    storages.push(storage);
                }
            }
            // (this might be a rather expensive approach to) mark nodes as members of an ordering
            let node_range: Range<u64> = 0..graph.get_node_annos().get_largest_item()?.unwrap();
            let node_annos = graph.get_node_annos();
            for node_id in node_range.filter(|n| {
                let r = node_annos.get_value_for_item(n, &NODE_TYPE_KEY);
                if let Ok(Some(v)) = r {
                    v == "node"
                } else {
                    false
                }
            }) {
                for storage in &storages {
                    if storage.has_outgoing_edges(node_id)?
                        || storage.get_ingoing_edges(node_id).next().is_some()
                    {
                        ordering_data.insert(node_id);
                        break;
                    }
                }
            }
        } else {
            return Err(AnnattoError::Export {
                reason: "Component `Ordering/annis/` is missing.".to_string(),
                exporter: self.module_name().to_string(),
                path: Path::new("./").to_path_buf(),
            });
        }
        Ok(())
    }
}

/// This function has a similar purpose compared to a `CycleSafeDFS` in inverse mode,
/// but can operate on multiple graph storages.
fn reachable_nodes(
    from_node: u64,
    storages: &Vec<Arc<dyn GraphStorage>>,
    retrieved: &mut BTreeSet<u64>,
    cache: &mut BTreeMap<u64, BTreeSet<u64>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if retrieved.contains(&from_node) {
        return Ok(());
    }
    retrieved.insert(from_node);
    if let Some(node_set) = cache.get(&from_node) {
        retrieved.extend(node_set);
    } else {
        for storage in storages {
            for in_going in storage.get_ingoing_edges(from_node) {
                let node = in_going?;
                reachable_nodes(node, storages, retrieved, cache)?;
            }
        }
    }
    Ok(())
}
