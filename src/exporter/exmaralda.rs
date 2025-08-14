use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{BufWriter, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    StepID, error::AnnattoError, importer::exmaralda::LANGUAGE_SEP, progress::ProgressReporter,
    util::Traverse,
};
use documented::{Documented, DocumentedFields};
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::GraphStorage,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_TYPE_KEY},
    types::AnnoKey,
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use quick_xml::{
    Writer,
    events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event},
};
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

/// Export [EXMARaLDA partition editor](https://exmaralda.org/en/partitur-editor-en/)
/// (`.exb`) files.
///
/// Example:
///
/// ```toml
/// [[export]]
/// format = "exmaralda"
/// path = "exb/MyCorpus"
///
/// [export.config]
/// copy_media = false
/// ```
#[derive(
    Facet, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize, Clone, PartialEq,
)]
#[serde(deny_unknown_fields)]
pub struct ExportExmaralda {
    /// If `true`, copy linked media files to the output location.
    ///
    /// Example:
    ///
    /// ```toml
    /// [export.config]
    /// copy_media = true
    /// ```
    #[serde(default)]
    copy_media: bool,
    /// Using this annotation key, the corpus nodes that define the entire subgraph relevant for a file are identified.
    /// The value will then be split by path delimiters and only the last segment is used.
    /// Example:
    ///
    /// ```toml
    /// [export.config]
    /// doc_anno = { ns = "annis", name = "node_name" }
    /// ```
    /// This defaults to `{ ns = "annis", name = "doc" }`.
    #[serde(default = "default_doc_key", with = "crate::estarde::anno_key")]
    doc_anno: AnnoKey,
    /// If there is a desired order in which the annotations should be displayed in EXMARaLDA,
    /// it can be set here by providing a list. Not specifying a namespace will not be interpreted
    /// as empty namespace, but will group all annotation names with any namespace sharing the
    /// provided name, together.
    /// Example:
    ///
    /// ```toml
    /// [export.config]
    /// tier_order = ["norm::norm", "dipl::dipl", "annotator"]
    /// ```
    #[serde(default, with = "crate::estarde::anno_key::in_sequence")]
    tier_order: Vec<AnnoKey>,
}

impl Default for ExportExmaralda {
    fn default() -> Self {
        Self {
            copy_media: Default::default(),
            doc_anno: default_doc_key(),
            tier_order: Default::default(),
        }
    }
}

fn default_doc_key() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

const MEDIA_DIR_NAME: &str = "media";
const SPEAKER_ANNO_NAMES: [&str; 7] = [
    "abbreviation",
    "sex",
    "id",
    "languages-used",
    "l1",
    "l2",
    "comment",
];

impl Exporter for ExportExmaralda {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut node_buffer = NodeData::default();
        let mut edge_buffer = EdgeData::default();
        self.traverse(&step_id, graph, &mut node_buffer, &mut edge_buffer)?;
        let (start_data, end_data, timeline_data, anno_data) = node_buffer;
        let (ordering_data, media_data) = edge_buffer;
        let doc_nodes = start_data.iter().map(|((d, _), _)| d).collect_vec();
        let node_annos = graph.get_node_annos();
        let media_dir_opt = if !media_data.is_empty() & self.copy_media {
            let d = output_path.join(MEDIA_DIR_NAME);
            fs::create_dir_all(d.clone())?;
            Some(d)
        } else {
            None
        };
        let progress = ProgressReporter::new(tx, step_id.clone(), doc_nodes.len())?;
        let extension = self.file_extension();
        for doc_node_id in doc_nodes {
            let doc_name =
                if let Some(dn) = node_annos.get_value_for_item(doc_node_id, &self.doc_anno)? {
                    dn
                } else {
                    continue;
                };
            let doc_path = output_path.join(format!(
                "{}.{extension}",
                doc_name.split(['/', '\\']).next_back().unwrap_or(&doc_name) // This always has a last
            ));
            if let Some(doc_parent) = doc_path.as_path().parent() {
                fs::create_dir_all(doc_parent)?;
            } else {
                return Err(Box::new(AnnattoError::Export {
                    reason: "Could not determine parent.".to_string(),
                    exporter: step_id.module_name.to_string(),
                    path: doc_path,
                }));
            }
            let file = fs::File::create(doc_path.as_path())?;
            let mut writer = Writer::new_with_indent(BufWriter::new(file), b' ', 2);
            writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;
            writer.write_event(Event::Start(BytesStart::new("basic-transcription")))?;
            writer.write_event(Event::Start(BytesStart::new("head")))?;
            writer.write_event(Event::Start(BytesStart::new("meta-information")))?;
            writer.create_element("project-name").write_empty()?;
            writer.create_element("transcription-name").write_empty()?;
            if let Some(paths) = media_data.get(doc_node_id) {
                for ref_path in paths {
                    let url = if self.copy_media {
                        if let (Some(media_dir), Some(media_file_name)) =
                            (&media_dir_opt, ref_path.file_name())
                        {
                            let target = media_dir.join(media_file_name);
                            fs::copy(ref_path, target)?;
                            Some(
                                Path::new(MEDIA_DIR_NAME)
                                    .join(media_file_name)
                                    .to_string_lossy()
                                    .to_string(),
                            )
                        } else {
                            None
                        }
                    } else {
                        // express path to original media file relative to newly created exmaralda file
                        if let Some(relative_path) =
                            pathdiff::diff_paths(env::current_dir()?.join(ref_path), output_path)
                        {
                            Some(relative_path.to_string_lossy().to_string())
                        } else {
                            return Err(Box::new(AnnattoError::Export {
                                reason: format!(
                                    "Could not derive relative path to media file {ref_path:?}",
                                ),
                                exporter: step_id.module_name.clone(),
                                path: doc_path.to_path_buf(),
                            }));
                        }
                    };
                    if let Some(url_str) = url {
                        writer
                            .create_element("referenced-file")
                            .with_attribute(("url", url_str.as_str()))
                            .write_empty()?;
                    }
                }
            };
            writer.create_element("ud-meta-information").write_empty()?;
            writer.create_element("comment").write_empty()?;
            writer
                .create_element("transcription-convention")
                .write_empty()?;
            writer.write_event(Event::End(BytesEnd::new("meta-information")))?;
            writer.write_event(Event::Start(BytesStart::new("speakertable")))?;
            // note: speaker id and speaker name are derived from namespaces,
            // since this is most compatible what most imported graphs will look like
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
                writer.write_event(Event::Text(BytesText::new(speaker_name)))?;
                writer.write_event(Event::End(BytesEnd::new("abbreviation")))?;
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
                writer
                    .create_element("sex")
                    .with_attribute(("value", sex_val.as_str()))
                    .write_empty()?;
                if let Some(v) = node_annos.get_value_for_item(
                    doc_node_id,
                    &AnnoKey {
                        name: "languages-used".into(),
                        ns: speaker_name.into(),
                    },
                )? {
                    writer.write_event(Event::Start(BytesStart::new("languages-used")))?;
                    for entry in v.split(LANGUAGE_SEP) {
                        writer
                            .create_element("language")
                            .with_attribute(("lang", entry.trim()))
                            .write_empty()?;
                    }
                    writer.write_event(Event::End(BytesEnd::new("languages-used")))?;
                } else {
                    writer.create_element("languages-used").write_empty()?;
                }
                for lang_key in ["l1", "l2"] {
                    if let Some(v) = node_annos.get_value_for_item(
                        doc_node_id,
                        &AnnoKey {
                            name: lang_key.into(),
                            ns: speaker_name.into(),
                        },
                    )? {
                        writer.write_event(Event::Start(BytesStart::new(lang_key)))?;
                        writer
                            .create_element("language")
                            .with_attribute(("lang", v.trim()))
                            .write_empty()?;
                        writer.write_event(Event::End(BytesEnd::new(lang_key)))?;
                    } else {
                        writer.create_element(lang_key).write_empty()?;
                    }
                }
                let mut user_defined_attrs = Vec::new();
                for anno_key in node_annos.get_all_keys_for_item(doc_node_id, None, None)? {
                    if anno_key.ns.as_str() == ANNIS_NS
                        || anno_key.ns.as_str() != speaker_name
                        || SPEAKER_ANNO_NAMES.contains(&anno_key.name.as_str())
                    {
                        continue;
                    }
                    if let Some(v) = node_annos.get_value_for_item(doc_node_id, &anno_key)? {
                        user_defined_attrs.push((anno_key.name.to_string(), v));
                    }
                }
                if user_defined_attrs.is_empty() {
                    writer
                        .create_element("ud-speaker-information")
                        .write_empty()?;
                } else {
                    writer.write_event(Event::Start(BytesStart::new("ud-speaker-information")))?;
                    for (attr_name, text_val) in user_defined_attrs {
                        writer
                            .create_element("ud-information")
                            .with_attribute(("attribute-name", attr_name.as_str()))
                            .write_text_content(BytesText::new(&text_val))?;
                    }
                    writer.write_event(Event::End(BytesEnd::new("ud-speaker-information")))?;
                }
                if let Some(v) = node_annos.get_value_for_item(
                    doc_node_id,
                    &AnnoKey {
                        name: "comment".into(),
                        ns: speaker_name.into(),
                    },
                )? {
                    writer.write_event(Event::Start(BytesStart::new("comment")))?;
                    writer.write_event(Event::Text(BytesText::new(&v)))?;
                    writer.write_event(Event::End(BytesEnd::new("comment")))?;
                } else {
                    writer.create_element("comment").write_empty()?;
                }
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
            for ((_, tli_id), t) in timeline.iter().sorted_by(|(_, ta), (_, tb)| ta.cmp(tb)) {
                writer
                    .create_element("tli")
                    .with_attribute(("id", tli_id.as_str()))
                    .with_attribute(("time", t.to_string().as_str()))
                    .write_empty()?;
            }
            writer.write_event(Event::End(BytesEnd::new("common-timeline")))?;
            for (i, anno_key) in node_annos
                .annotation_keys()?
                .iter()
                .sorted_unstable_by(|a, b| {
                    let position_a = self
                        .tier_order
                        .iter()
                        .position(|k| (k.ns == a.ns || k.ns.is_empty()) && a.name == k.name)
                        .unwrap_or(usize::MAX);
                    let position_b = self
                        .tier_order
                        .iter()
                        .position(|k| (k.ns == b.ns || k.ns.is_empty()) && b.name == k.name)
                        .unwrap_or(usize::MAX);
                    position_a.cmp(&position_b)
                })
                .enumerate()
            {
                if anno_key.ns == ANNIS_NS && anno_key.name != "tok" {
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
                        if let (Some(start_a), Some(start_b)) = (
                            start_data.get(&(*doc_node_id, node_a)),
                            start_data.get(&(*doc_node_id, node_b)),
                        ) {
                            if let (Some(time_a), Some(time_b)) = (
                                timeline.get(&(*doc_node_id, start_a.to_string())),
                                timeline.get(&(*doc_node_id, start_b.to_string())),
                            ) {
                                time_a.total_cmp(time_b)
                            } else {
                                Ordering::Equal // will never happen
                            }
                        } else {
                            Ordering::Equal // will never happen
                        }
                    });
                    let tier_type = if let Some((node_id, _)) = entries.last()
                        && ((anno_key.ns == ANNIS_NS && &anno_key.name == "tok")
                            || ordering_data
                                .get(node_id)
                                .map(|o_name| o_name == &anno_key.name)
                                .unwrap_or_default())
                    {
                        "t"
                    } else {
                        "a"
                    };
                    let display_name =
                        format!("{}[{}]", anno_key.ns.as_str(), anno_key.name.as_str());
                    let tier_id = format!("TIER{i}");
                    let tier_attributes = [
                        (
                            "speaker",
                            if anno_key.ns == ANNIS_NS {
                                ""
                            } else {
                                anno_key.ns.as_str()
                            },
                        ),
                        ("category", anno_key.name.as_str()),
                        ("type", tier_type),
                        ("id", tier_id.as_str()),
                        ("display-name", display_name.as_str()),
                    ];
                    let tier = BytesStart::new("tier").with_attributes(tier_attributes);
                    writer.write_event(Event::Start(tier))?;
                    for (node_id, anno_value) in sorted_entries {
                        if let Some(start) = start_data.get(&(*doc_node_id, *node_id))
                            && let Some(end) = end_data.get(&(*doc_node_id, *node_id))
                        {
                            let mut event = BytesStart::new("event");
                            event.push_attribute(("start", start.as_str()));
                            event.push_attribute(("end", end.as_str()));
                            writer.write_event(Event::Start(event))?;
                            writer.write_event(Event::Text(BytesText::new(anno_value)))?;
                            writer.write_event(Event::End(BytesEnd::new("event")))?;
                        }
                    }
                    writer.write_event(Event::End(BytesEnd::new("tier")))?;
                }
            }
            writer.write_event(Event::End(BytesEnd::new("basic-body")))?;
            writer.write_event(Event::End(BytesEnd::new("basic-transcription")))?;
            writer.into_inner().flush()?;
            progress.worked(1)?;
        }
        Ok(())
    }

    fn file_extension(&self) -> &str {
        "exb"
    }
}

type NodeData = (TimeData, TimeData, TimelineData, AnnoData);
type TimeData = BTreeMap<(u64, u64), String>;
type AnnoData = BTreeMap<(u64, (String, String)), Vec<(u64, String)>>;
type TimelineData = BTreeMap<(u64, String), OrderedFloat<f32>>;
type OrderingData = BTreeMap<u64, String>; // node ids in this set are member of an ordering (relevant to determine tier type)
type AudioData = BTreeMap<u64, Vec<PathBuf>>; // maps document nodes to linked files
type EdgeData = (OrderingData, AudioData);

impl Traverse<NodeData, EdgeData> for ExportExmaralda {
    fn node(
        &self,
        _step_id: &StepID,
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
        _step_id: &StepID,
        _graph: &AnnotationGraph,
        _edge: graphannis_core::types::Edge,
        _component: &graphannis::model::AnnotationComponent,
        _buffer: &mut EdgeData,
    ) -> crate::error::Result<()> {
        Ok(())
    }

    fn traverse(
        &self,
        step_id: &StepID,
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
                .filter_map(|r| match r {
                    Ok(n) => {
                        if !storage.has_ingoing_edges(n).unwrap_or_default() {
                            Some(n)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                })
                .collect_vec();
            // map all roots to a document
            let part_of_c = AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            );
            let part_of_storage = if let Some(strg) = graph.get_graphstorage(&part_of_c) {
                strg
            } else {
                return Err(AnnattoError::Export { reason: "Graph has no part of component, which makes it impossible to identify document nodes.".to_string(), exporter: step_id.module_name.to_string(), path: step_id.clone().path.unwrap_or(Path::new("./").to_path_buf()) });
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
                    for step in dfs.flatten() {
                        let node = step.node;
                        let terminate = graph
                            .get_node_annos()
                            .has_value_for_item(&node, &self.doc_anno);
                        if terminate.unwrap_or_default() {
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
            let (ordering_data, audio_data) = edge_buffer;
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
                        exporter: step_id.module_name.clone(),
                        path: Path::new("./").to_path_buf(),
                    })?;
                    for n in &covering_nodes {
                        let k = (doc_node, *n);
                        start_at_tli.entry(k).or_insert(tli_id.to_string());
                        end_at_tli.insert(k, next_tli_id.to_string());
                        if !processed_nodes.contains(n) {
                            graph
                                .get_node_annos()
                                .get_annotations_for_item(n)
                                .map_err(|_| AnnattoError::Export {
                                    reason: "Could not gather annotations for a node.".to_string(),
                                    exporter: step_id.module_name.clone(),
                                    path: Path::new("./").to_path_buf(),
                                })?
                                .into_iter()
                                .for_each(|a| {
                                    // collect annotations
                                    if a.key.ns != ANNIS_NS {
                                        let anno_k = (
                                            doc_node,
                                            (a.key.ns.to_string(), a.key.name.to_string()),
                                        );
                                        if let Some(data) = anno_data.get_mut(&anno_k) {
                                            data.push((*n, a.val.to_string()));
                                        } else {
                                            anno_data.insert(anno_k, vec![(*n, a.val.to_string())]);
                                        }
                                    }
                                });
                            // check for interval annotations
                            if let Ok(Some(interval)) =
                                graph.get_node_annos().get_value_for_item(n, &time_key)
                                && let Some(tpl) = interval.split_once('-')
                            {
                                for time_string in [tpl.0, tpl.1] {
                                    let time = time_string
                                            .parse::<OrderedFloat<f32>>()
                                            .map_err(|_| AnnattoError::Export {
                                                reason: format!("Failed to parse time value {time_string} of interval {interval}"),
                                                exporter: step_id.module_name.clone(),
                                                path: Path::new("./").to_path_buf(),
                                            })?;
                                    time_values.insert(time);
                                }
                            }
                            processed_nodes.insert(*n);
                        }
                    }
                    if let Some(tok_value) = graph.get_node_annos().get_value_for_item(
                        &timeline_token,
                        &AnnoKey {
                            name: "tok".into(),
                            ns: ANNIS_NS.into(),
                        },
                    )? && !tok_value.trim().is_empty()
                    {
                        let k = (doc_node, (ANNIS_NS.to_string(), "tok".to_string()));
                        let v = (timeline_token, tok_value.to_string());
                        match anno_data.entry(k) {
                            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(vec![v]);
                            }
                            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                                occupied_entry.get_mut().push(v);
                            }
                        };
                    }
                }
                if !time_values.is_empty() {
                    for (i, t) in (0..max_dist + 2).zip(time_values.into_iter().sorted()) {
                        tli2time.insert((doc_node, format!("T{i}")), t);
                    }
                }
                // find potentially linked audio file
                let mut media_vec = Vec::new();
                for sn in part_of_storage.get_ingoing_edges(doc_node) {
                    let source_node_id = sn?;
                    if let Some(node_type_value) = graph
                        .get_node_annos()
                        .get_value_for_item(&source_node_id, &NODE_TYPE_KEY)?
                        && node_type_value == "file"
                        && let Some(path_value) = graph.get_node_annos().get_value_for_item(
                            &source_node_id,
                            &AnnoKey {
                                name: "file".into(),
                                ns: ANNIS_NS.into(),
                            },
                        )?
                    {
                        let path = Path::new(&*path_value);
                        media_vec.push(path.to_path_buf());
                    }
                }
                audio_data.insert(doc_node, media_vec);
            }
            // collecting named ordering data
            let mut storages = Vec::new();
            for ordering in graph.get_all_components(Some(AnnotationComponentType::Ordering), None)
            {
                if ordering.name.is_empty() {
                    continue;
                }
                if let Some(storage) = graph.get_graphstorage(&ordering) {
                    storages.push((storage, ordering.name.to_string()));
                }
            }
            // (this might be a rather expensive approach to) mark nodes as members of an ordering
            let node_range: Range<u64> = 0..graph
                .get_node_annos()
                .get_largest_item()?
                .unwrap_or(u64::MAX); // let's hope it'll never be necessary to use u64::MAX
            let node_annos = graph.get_node_annos();
            for node_id in node_range.filter(|n| {
                let r = node_annos.get_value_for_item(n, &NODE_TYPE_KEY);
                if let Ok(Some(v)) = r {
                    v == "node"
                } else {
                    false
                }
            }) {
                for (storage, o_name) in &storages {
                    if storage.has_outgoing_edges(node_id)? || storage.has_ingoing_edges(node_id)? {
                        ordering_data.insert(node_id, o_name.to_string());
                        break;
                    }
                }
            }
        } else {
            return Err(AnnattoError::Export {
                reason: "Component `Ordering/annis/` is missing.".to_string(),
                exporter: step_id.module_name.clone(),
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

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::{Path, PathBuf},
    };

    use graphannis::{AnnotationGraph, graph::AnnoKey};
    use insta::assert_snapshot;
    use tempfile::TempDir;

    use crate::{
        ImporterStep, ReadFrom, StepID,
        exporter::exmaralda::ExportExmaralda,
        importer::{Importer, exmaralda::ImportEXMARaLDA, treetagger::ImportTreeTagger},
        test_util::{export_to_string, export_to_string_in_directory},
    };

    #[test]
    fn serialize() {
        let module = ExportExmaralda::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = ExportExmaralda {
            copy_media: true,
            doc_anno: AnnoKey {
                ns: "annis".into(),
                name: "not_doc".into(),
            },
            tier_order: vec![
                AnnoKey {
                    ns: "dipl".into(),
                    name: "dipl".into(),
                },
                AnnoKey {
                    ns: "norm".into(),
                    name: "norm".into(),
                },
            ],
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn flat_data() {
        let m: Result<ImportTreeTagger, _> = toml::from_str("attribute_decoding = \"entities\"");
        assert!(m.is_ok());
        let import = m.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/import/treetagger/single_sentence/zossen.tt"),
            StepID {
                module_name: "test_import".into(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let exporter: Result<ExportExmaralda, _> = toml::from_str("");
        assert!(exporter.is_ok());
        let actual = export_to_string(&graph, exporter.unwrap());
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn test_exmaralda_export() {
        let import = ImportEXMARaLDA::default();
        let step = ImporterStep {
            module: crate::ReadFrom::EXMARaLDA(import),
            path: PathBuf::from("./tests/data/import/exmaralda/clean/import/"),
        };
        let u = step.execute(None);
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());

        let output_path = TempDir::new().unwrap();

        let actual =
            export_to_string_in_directory(&graph, ExportExmaralda::default(), &output_path);
        assert!(actual.is_ok());

        let path_to_remove =
            pathdiff::diff_paths(std::env::current_dir().unwrap(), output_path.path()).unwrap();
        let path_to_remove = path_to_remove.to_str().unwrap();
        insta::with_settings!({filters => vec![
            (path_to_remove, "[GRAPH_DIR]"),
        ]}, {
            assert_snapshot!(actual.unwrap());
        });
    }

    #[test]
    fn test_exmaralda_export_ordered() {
        let import = ImportEXMARaLDA::default();
        let step = ImporterStep {
            module: crate::ReadFrom::EXMARaLDA(import),
            path: PathBuf::from("./tests/data/import/exmaralda/clean/import/"),
        };
        let u = step.execute(None);
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());

        let output_path = TempDir::new().unwrap();
        let exporter: Result<ExportExmaralda, _> =
            toml::from_str("tier_order = [\"sentence\", \"dipl\", \"norm\"]");
        assert!(exporter.is_ok(), "Error: {:?}", exporter.err());
        let actual = export_to_string_in_directory(&graph, exporter.unwrap(), &output_path);
        assert!(actual.is_ok());

        let path_to_remove =
            pathdiff::diff_paths(std::env::current_dir().unwrap(), output_path.path()).unwrap();
        let path_to_remove = path_to_remove.to_str().unwrap();
        insta::with_settings!({filters => vec![
            (path_to_remove, "[GRAPH_DIR]"),
        ]}, {
            assert_snapshot!(actual.unwrap());
        });
    }

    #[test]
    fn test_exmaralda_export_with_audio() {
        let import = ReadFrom::EXMARaLDA(ImportEXMARaLDA::default());

        let wd = env::current_dir();
        assert!(wd.is_ok());
        let source_path = wd
            .unwrap()
            .join(Path::new("./tests/data/import/exmaralda/clean/import/"));

        let step = ImporterStep {
            module: import,
            path: source_path,
        };
        let u = step.execute(None);

        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let actual = export_to_string(
            &graph,
            ExportExmaralda {
                copy_media: true,
                ..Default::default()
            },
        );
        assert!(actual.is_ok());

        assert_snapshot!(actual.unwrap());
    }
}
