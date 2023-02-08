use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::*;

use crate::models::textgrid::{Interval, TextGrid, TextGridItem};
use crate::progress::ProgressReporter;
use crate::util::graphupdate::{
    add_order_relations, map_annotations, map_audio_source, map_token, path_structure,
};
use crate::Module;
use anyhow::{anyhow, Result};
use encoding_rs_io::DecodeReaderBytes;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use ordered_float::OrderedFloat;

use super::Importer;
const FILE_ENDINGS: [&str; 3] = ["textgrid", "TextGrid", "textGrid"];
const PROP_TIER_GROUPS: &str = "tier_groups";
const PROP_MAP_TIMELINE: &str = "map_timeline";
const PROP_AUDIO_EXTENSION: &str = "audio_extension";
const PROP_SKIP_AUDIO: &str = "skip_audio";
const PROP_SKIP_TIME_ANNOS: &str = "skip_time_annotations";

/// Importer the Praat TextGrid file format.
///
/// See the [Praat
/// Documentation](https://www.fon.hum.uva.nl/praat/manual/TextGrid_file_formats.html)
/// for more information on the format itself.
#[derive(Default)]
pub struct TextgridImporter {}

impl Module for TextgridImporter {
    fn module_name(&self) -> &str {
        "TextgridImporter"
    }
}

struct MapperParams<'a> {
    tier_groups: BTreeMap<&'a str, BTreeSet<&'a str>>,
    map_timeline: bool,
    audio_extension: &'a str,
    skip_audio: bool,
    skip_time_annotations: bool,
}

fn parse_tier_map(value: &str) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut tier_map = BTreeMap::new();
    for group in value.split(";") {
        if let Some((owner, objects)) = group.split_once("={") {
            let owner = owner.trim();
            if objects.len() > 0 {
                let value: BTreeSet<_> = objects[0..(objects.len() - 1)]
                    .split(",")
                    .map(|e| e.trim())
                    .filter(|e| !e.is_empty())
                    .collect();
                tier_map.insert(owner, value);
            }
        }
    }
    return tier_map;
}

struct DocumentMapper<'a> {
    doc_path: String,
    text_node_name: String,
    textgrid: TextGrid,
    reporter: &'a ProgressReporter,
    file_path: PathBuf,
    params: &'a MapperParams<'a>,
    number_of_spans: usize,
}

impl<'a> DocumentMapper<'a> {
    fn map(&mut self, u: &mut GraphUpdate) -> Result<()> {
        // Add a subcorpus like node for the text
        u.add_event(UpdateEvent::AddNode {
            node_name: self.text_node_name.clone(),
            node_type: "datasource".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: self.text_node_name.clone(),
            target_node: self.doc_path.clone(),
            layer: ANNIS_NS.to_string(),
            component_type: "PartOf".to_string(),
            component_name: "".to_string(),
        })?;

        if !self.params.skip_audio {
            // TODO: Check assumption that the audio file is always relative to the actual file
            let audio_path = self.file_path.with_extension(self.params.audio_extension);
            if audio_path.exists() {
                map_audio_source(u, &audio_path, &self.doc_path)?;
            } else {
                self.reporter.info(&format!(
                    "Could not find corresponding audio file {}",
                    audio_path.to_string_lossy()
                ))?;
            }
        }

        let mut time_to_id = if self.params.map_timeline {
            let token_tier_names: BTreeSet<_> =
                self.params.tier_groups.keys().map(|n| *n).collect();
            let valid_tier_names = if token_tier_names.is_empty() {
                // Add all tiers
                None
            } else {
                // Only include the token tiers
                Some(token_tier_names)
            };
            self.map_timeline_from_timecode(u, valid_tier_names.as_ref())?
        } else {
            self.map_timeline_from_token_tier(u)?
        };

        for tok_tier_name in self.params.tier_groups.keys() {
            self.map_tier_group(u, tok_tier_name, &mut time_to_id, self.params.map_timeline)?;
        }

        Ok(())
    }

    fn map_timeline_from_timecode(
        &self,
        u: &mut GraphUpdate,
        valid_tier_names: Option<&BTreeSet<&str>>,
    ) -> Result<BTreeMap<OrderedFloat<f64>, String>> {
        // Collect all points of time based on the intervals and points.
        let mut existing_points_of_times: BTreeSet<OrderedFloat<f64>> = BTreeSet::default();
        for tier in self.textgrid.items.iter() {
            match tier {
                TextGridItem::Interval {
                    intervals, name, ..
                } => {
                    let include_tier = valid_tier_names
                        .map(|valid| valid.contains(name.as_str()))
                        .unwrap_or(true);
                    if include_tier {
                        for i in intervals {
                            existing_points_of_times.insert(i.xmin.into());
                            existing_points_of_times.insert(i.xmax.into());
                        }
                    }
                }
                TextGridItem::Text { points, name, .. } => {
                    let include_tier = valid_tier_names
                        .map(|valid| valid.contains(name.as_str()))
                        .unwrap_or(true);
                    if include_tier {
                        for p in points {
                            existing_points_of_times.insert(p.number.into());
                        }
                    }
                }
            }
        }
        let mut tli_names = Vec::new();
        let mut result = BTreeMap::new();
        // Add a token for each interval between each point of time and remember
        // its name. Since the set is sorted by time, we can just iterate over
        // it in the correct order.
        let mut it = existing_points_of_times.iter().peekable();
        let mut counter = 1;
        while let Some(current_pot) = it.next() {
            let start = if self.params.skip_time_annotations {
                None
            } else {
                Some(current_pot.0)
            };
            let mut end = None;
            if !self.params.skip_time_annotations {
                if let Some(next_pot) = it.peek() {
                    end = Some(next_pot.0);
                }
            }
            let tli_id = map_token(
                u,
                &self.doc_path,
                &self.text_node_name,
                &counter.to_string(),
                None,
                "",
                start,
                end,
                false,
            )?;
            tli_names.push(tli_id.clone());
            result.insert(*current_pot, tli_id);
            counter += 1;
        }
        add_order_relations(u, &tli_names, None)?;

        Ok(result)
    }

    fn map_timeline_from_token_tier(
        &self,
        u: &mut GraphUpdate,
    ) -> Result<BTreeMap<OrderedFloat<f64>, String>> {
        // One can only map without a timeline if there is a single token
        // layer explicitily defined by the tier_group property.
        if self.params.tier_groups.len() > 1 {
            return Err(anyhow!("Only one token tier can be defined in tier_groups when mapping without a timeline (map_timeline=false)."));
        } else if let Some((token_tier_name, _)) = self.params.tier_groups.iter().next() {
            let mut token_sorted_by_time = BTreeMap::default();

            for tier in self.textgrid.items.iter() {
                match tier {
                    TextGridItem::Interval {
                        name, intervals, ..
                    } => {
                        if name == token_tier_name {
                            for i in intervals {
                                token_sorted_by_time.insert(
                                    (OrderedFloat(i.xmin), OrderedFloat(i.xmax)),
                                    i.text.clone(),
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
            let mut token_ids = Vec::new();
            let mut result = BTreeMap::new();
            let mut counter = 1;
            for (time_range, token_text) in token_sorted_by_time {
                let id = map_token(
                    u,
                    &self.doc_path,
                    &self.text_node_name,
                    &counter.to_string(),
                    None,
                    &token_text,
                    Some(time_range.0 .0),
                    Some(time_range.1 .0),
                    true,
                )?;

                token_ids.push(id.clone());
                result.insert(time_range.0, id.clone());
                result.insert(time_range.1, id);
                counter += 1;
            }
            add_order_relations(u, &token_ids, None)?;

            Ok(result)
        } else {
            return Err(
                anyhow!("Exactly one token tier must be definied in tier_groups when mapping without a timeline (map_timeline=false"),
            );
        }
    }

    fn map_tier_group(
        &mut self,
        u: &mut GraphUpdate,
        tok_tier_name: &str,
        time_to_id: &mut BTreeMap<OrderedFloat<f64>, String>,
        map_token_tier: bool,
    ) -> Result<()> {
        if map_token_tier {
            let segmentation_span_ids =
                self.map_annotation_tier(u, tok_tier_name, None, true, time_to_id)?;

            add_order_relations(u, &segmentation_span_ids, Some(tok_tier_name))?;
        }

        if let Some(dependent_tier_names) = self.params.tier_groups.get(tok_tier_name) {
            for tier in dependent_tier_names {
                self.map_annotation_tier(u, tier, Some(tok_tier_name), false, time_to_id)?;
            }
        }
        Ok(())
    }

    fn map_annotation_tier(
        &mut self,
        u: &mut GraphUpdate,
        tier_name: &str,
        parent_tier_name: Option<&str>,
        is_segmentation: bool,
        time_to_id: &BTreeMap<OrderedFloat<f64>, String>,
    ) -> Result<Vec<String>> {
        let mut node_ids_sorted = BTreeMap::default();

        let tier = self.textgrid.items.iter().find(|item| match item {
            TextGridItem::Interval { name, .. } | TextGridItem::Text { name, .. } => {
                tier_name == name
            }
        });

        let parent_tier_intervals = parent_tier_name.and_then(|tier_name| {
            for item in self.textgrid.items.iter() {
                match item {
                    TextGridItem::Interval {
                        name, intervals, ..
                    } => {
                        if name == tier_name {
                            let mut intervals = intervals.clone();
                            // Make sure the intervals are sorted by their start time
                            intervals.sort_unstable_by_key(|i| OrderedFloat(i.xmin));
                            return Some(intervals);
                        }
                    }
                    _ => {}
                }
            }
            None
        });

        if let Some(tier) = tier {
            match tier {
                TextGridItem::Interval {
                    name, intervals, ..
                } => {
                    for i in intervals {
                        if !i.text.trim().is_empty() {
                            let (start, end) = best_matching_start_end(i, &parent_tier_intervals);

                            let span_id =
                                self.add_span(u, &name, &i.text, start, end, time_to_id)?;
                            if is_segmentation {
                                u.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: span_id.clone(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "tok".to_string(),
                                    anno_value: i.text.clone(),
                                })?;
                            }
                            node_ids_sorted.insert(OrderedFloat(i.xmin), span_id);
                            self.number_of_spans += 1;
                        }
                    }
                }
                TextGridItem::Text { name, points, .. } => {
                    for p in points {
                        if !p.mark.trim().is_empty() {
                            let time = OrderedFloat(p.number);
                            let overlapped: Vec<_> =
                                time_to_id.range(time..=time).map(|(_k, v)| v).collect();
                            let span_id = map_annotations(
                                u,
                                &self.doc_path,
                                &self.text_node_name,
                                &(self.number_of_spans + 1).to_string(),
                                None,
                                Some(&name),
                                Some(&p.mark),
                                &overlapped,
                            )?;

                            node_ids_sorted.insert(OrderedFloat(p.number), span_id);
                            self.number_of_spans += 1;
                        }
                    }
                }
            }
        } else {
            self.reporter
                .warn(&format!("Missing tier with name '{}'", tier_name))?;
        }
        Ok(node_ids_sorted
            .into_iter()
            .map(|(_, span_id)| span_id)
            .collect_vec())
    }

    fn add_span(
        &self,
        u: &mut GraphUpdate,
        anno_name: &str,
        anno_value: &str,
        start_time: f64,
        end_time: f64,
        time_to_token_id: &BTreeMap<OrderedFloat<f64>, String>,
    ) -> Result<String> {
        let start_time = OrderedFloat(start_time);
        let end_time = OrderedFloat(end_time);

        let overlapped: Vec<_> = time_to_token_id
            .range(start_time..end_time)
            .map(|(_k, v)| v)
            .collect();
        let id = map_annotations(
            u,
            &self.doc_path,
            &self.text_node_name,
            &(self.number_of_spans + 1).to_string(),
            None,
            Some(&anno_name),
            Some(&anno_value),
            &overlapped,
        )?;
        Ok(id)
    }
}

impl Importer for TextgridImporter {
    fn import_corpus(
        &self,
        input_path: &Path,
        properties: &collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();
        let tier_groups = parse_tier_map(properties.get(PROP_TIER_GROUPS).ok_or_else(|| {
            anyhow!(
                "No tier mapping configurated (property \"{}\" missing). Cannot proceed.",
                PROP_TIER_GROUPS
            )
        })?);
        let params = MapperParams {
            tier_groups,
            map_timeline: properties
                .get(PROP_MAP_TIMELINE)
                .map_or(true, |v| v.trim().eq_ignore_ascii_case("true")),
            skip_audio: properties
                .get(PROP_SKIP_AUDIO)
                .map_or(false, |v| v.trim().eq_ignore_ascii_case("true")),
            skip_time_annotations: properties
                .get(PROP_SKIP_TIME_ANNOS)
                .map_or(false, |v| v.trim().eq_ignore_ascii_case("true")),
            audio_extension: properties
                .get(PROP_AUDIO_EXTENSION)
                .map_or("wav", |ext| ext.as_str()),
        };

        let documents = path_structure(&mut u, input_path, &FILE_ENDINGS)?;
        let reporter =
            ProgressReporter::new(tx, self as &dyn Module, Some(input_path), documents.len())?;
        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            // Some TextGrid files are not UTF-8, but UTF-16, so use a reader
            // that uses the BOM and can transcode the file content if
            // necessary.
            let f = std::fs::File::open(&file_path)?;
            let mut decoder = DecodeReaderBytes::new(f);
            let mut file_content = String::new();
            decoder.read_to_string(&mut file_content)?;

            let textgrid = TextGrid::parse(&file_content)?;

            let text_node_name = format!("{}#text", &doc_path);

            let mut doc_mapper = DocumentMapper {
                doc_path,
                textgrid,
                reporter: &reporter,
                file_path,
                params: &params,
                number_of_spans: 0,
                text_node_name,
            };

            doc_mapper.map(&mut u)?;
            reporter.worked(1)?;
        }
        Ok(u)
    }
}

/// Find the token that this span belongs to and use its time code instead
/// of the original one
fn best_matching_start_end(
    orig_interval: &Interval,
    parent_tier_intervals: &Option<Vec<Interval>>,
) -> (f64, f64) {
    let mut start = orig_interval.xmin;
    let mut end = orig_interval.xmax;
    if let Some(parent_tier_intervals) = &parent_tier_intervals {
        if let Err(insertion_idx) = parent_tier_intervals
            .binary_search_by_key(&OrderedFloat(orig_interval.xmin), |interval| {
                OrderedFloat(interval.xmin)
            })
        {
            let upper_candidate = &parent_tier_intervals[insertion_idx];
            start = upper_candidate.xmin;
            if let Some(lower_candidate) = &parent_tier_intervals.get(insertion_idx - 1) {
                // Decide based on which candidate is nearer
                if (orig_interval.xmin - lower_candidate.xmin).abs() < (orig_interval.xmin - upper_candidate.xmin).abs() {
                    start = lower_candidate.xmin;
                }
            }
        }
        if let Err(insertion_idx) = parent_tier_intervals
            .binary_search_by_key(&OrderedFloat(orig_interval.xmax), |interval| {
                OrderedFloat(interval.xmax)
            })
        {
            let upper_candidate = &parent_tier_intervals[insertion_idx];
            end = upper_candidate.xmax;
            if let Some(lower_candidate) = &parent_tier_intervals.get(insertion_idx - 1) {
                // Decide based on which candidate is nearer
                if (orig_interval.xmax - lower_candidate.xmax).abs() < (orig_interval.xmax - upper_candidate.xmax).abs() {
                    end = lower_candidate.xmax;
                }
            }
        }
    }
    (start, end)
}

#[cfg(test)]
mod tests;
