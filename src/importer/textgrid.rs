use std::collections::{BTreeMap, BTreeSet};
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::path::PathBuf;
use std::*;

use crate::models::textgrid::{TextGrid, TextGridItem};
use crate::progress::ProgressReporter;
use crate::util::graphupdate::{
    add_order_relations, map_annotations, map_audio_source, map_token, path_structure,
};
use crate::Module;
use anyhow::{anyhow, Result};
use encoding_rs_io::DecodeReaderBytes;
use graphannis::update::GraphUpdate;
use ordered_float::OrderedFloat;

use super::Importer;
const _FILE_ENDINGS: [&str; 3] = ["textgrid", "TextGrid", "textGrid"];
const _FILE_TYPE_SHORT: &str = "ooTextFile short";
const _FILE_TYPE_LONG: &str = "ooTextFile";
const _TIER_CLASS_INTERVAL: &str = "IntervalTier";
const _TIER_CLASS_POINT: &str = "PointTier";
const _PROP_TIER_GROUPS: &str = "tier_groups";
const _PROP_FORCE_MULTI_TOK: &str = "force_multi_tok";
const _PROP_AUDIO_EXTENSION: &str = "audio_extension";
const _PROP_SKIP_AUDIO: &str = "skip_audio";
const _PROP_SKIP_TIME_ANNOS: &str = "skip_time_annotations";

/// Importer for some of the Praat TextGrid file formats.
///
///  See the [Praat
/// Documentation](https://www.fon.hum.uva.nl/praat/manual/TextGrid_file_formats.html)
/// for more information on the format(s) itself.
#[derive(Default)]
pub struct TextgridImporter {}

impl Module for TextgridImporter {
    fn module_name(&self) -> &str {
        "TextgridImporter"
    }
}

struct MapperParams<'a> {
    tier_groups: BTreeMap<&'a str, BTreeSet<&'a str>>,
    force_multi_tok: bool,
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
    textgrid: TextGrid,
    reporter: &'a ProgressReporter,
    file_path: PathBuf,
    params: &'a MapperParams<'a>,
    number_of_spans: usize,
}

impl<'a> DocumentMapper<'a> {
    fn map(&mut self, u: &mut GraphUpdate) -> Result<()> {
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

        let is_multi_tok = self.params.tier_groups.len() > 1 || self.params.force_multi_tok;
        let time_to_token_id = if is_multi_tok {
            let time_to_index = self.map_timeline(u)?;
            time_to_index
        } else {
            BTreeMap::default()
        };

        for tok_tier_name in self.params.tier_groups.keys() {
            self.map_tier_group(u, tok_tier_name, &time_to_token_id)?;
        }

        Ok(())
    }

    fn map_timeline(&self, u: &mut GraphUpdate) -> Result<BTreeMap<OrderedFloat<f64>, String>> {
        // Collect all points of time based on the intervals and points.
        // Sort them by using a sorted set.
        let mut existing_points_of_times: BTreeSet<OrderedFloat<f64>> = BTreeSet::default();
        for tier in self.textgrid.items.iter() {
            match tier {
                TextGridItem::Interval { intervals, .. } => {
                    for i in intervals {
                        existing_points_of_times.insert(i.xmin.into());
                        existing_points_of_times.insert(i.xmax.into());
                    }
                }
                TextGridItem::Text { points, .. } => {
                    for p in points {
                        existing_points_of_times.insert(p.number.into());
                    }
                }
            }
        }
        let mut tli_names = Vec::new();
        let mut result = BTreeMap::new();
        // Add a token for each point of time and remember its name
        let mut it = existing_points_of_times.iter().peekable();
        let mut counter = 1;
        while let Some(pot) = it.next() {
            let current_token_time = if self.params.skip_time_annotations {
                None
            } else {
                Some(pot.0)
            };
            let next_token_time = if self.params.skip_time_annotations {
                None
            } else {
                it.peek().map(|t| t.0)
            };

            let tli_id = map_token(
                u,
                &self.doc_path,
                &counter.to_string(),
                None,
                "",
                current_token_time,
                next_token_time,
                false,
            )?;
            tli_names.push(tli_id.clone());
            result.insert(*pot, tli_id);
            counter += 1;
        }
        add_order_relations(u, &tli_names, None)?;

        Ok(result)
    }
    fn map_token_tier(&self, u: &mut GraphUpdate, tok_tier_name: &str) -> Result<()> {
        // Find the tier matching the name from the configuration
        let tok_tier = self
            .textgrid
            .items
            .iter()
            .filter_map(|item| match item {
                TextGridItem::Interval { name, .. } => {
                    if name == tok_tier_name {
                        Some(item)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .next();
        if let Some(TextGridItem::Interval {
            name,
            xmin,
            xmax,
            intervals,
        }) = tok_tier
        {
            let start_time = if self.params.skip_time_annotations {
                None
            } else {
                Some(*xmin)
            };
            let end_time = if self.params.skip_time_annotations {
                None
            } else {
                Some(*xmax)
            };
            // Each interval of the tier is a token
            let mut token_ids = Vec::default();
            for (token_idx, i) in intervals.iter().enumerate() {
                let id = map_token(
                    u,
                    &self.doc_path,
                    &format!("{}{}", tok_tier_name, token_idx),
                    Some(name),
                    &i.text,
                    start_time,
                    end_time,
                    true,
                )?;
                token_ids.push(id);
            }
            // If there this document has multiple tokenizations add named order
            // relations
            let seg = if self.params.tier_groups.len() > 1 || self.params.force_multi_tok {
                Some(tok_tier_name)
            } else {
                None
            };
            add_order_relations(u, &token_ids, seg)?;
        }
        Ok(())
    }

    fn map_annotation_tier(
        &mut self,
        u: &mut GraphUpdate,
        tier_name: &str,
        time_to_token_id: &BTreeMap<OrderedFloat<f64>, String>,
    ) -> Result<()> {
        // TODO: correct the time codes
        let tier = self.textgrid.items.iter().find(|item| match item {
            TextGridItem::Interval { name, .. } | TextGridItem::Text { name, .. } => {
                tier_name == name
            }
        });
        if let Some(tier) = tier {
            match tier {
                TextGridItem::Interval {
                    name, intervals, ..
                } => {
                    for i in intervals {
                        if !i.text.trim().is_empty() {
                            let start = OrderedFloat(i.xmin);
                            let end = OrderedFloat(i.xmax);
                            let overlapped: Vec<_> = time_to_token_id
                                .range(start..=end)
                                .map(|(_k, v)| v)
                                .collect();
                            map_annotations(
                                u,
                                &self.doc_path,
                                &(self.number_of_spans + 1).to_string(),
                                None,
                                Some(&name),
                                Some(&i.text),
                                &overlapped,
                            )?;
                            self.number_of_spans += 1;
                        }
                    }
                }
                TextGridItem::Text { name, points, .. } => {
                    for p in points {
                        if !p.mark.trim().is_empty() {
                            let time = OrderedFloat(p.number);
                            let overlapped: Vec<_> = time_to_token_id
                                .range(time..=time)
                                .map(|(_k, v)| v)
                                .collect();
                            map_annotations(
                                u,
                                &self.doc_path,
                                &(self.number_of_spans + 1).to_string(),
                                None,
                                Some(&name),
                                Some(&p.mark),
                                &overlapped,
                            )?;
                            self.number_of_spans += 1;
                        }
                    }
                }
            }
        } else {
            self.reporter
                .warn(&format!("Missing tier with name '{}'", tier_name))?;
        }
        Ok(())
    }

    fn map_tier_group(
        &mut self,
        u: &mut GraphUpdate,
        tok_tier_name: &str,
        time_to_token_id: &BTreeMap<OrderedFloat<f64>, String>,
    ) -> Result<()> {
        self.map_token_tier(u, tok_tier_name)?;
        if let Some(dependent_tier_names) = self.params.tier_groups.get(tok_tier_name) {
            for tier in dependent_tier_names {
                self.map_annotation_tier(u, tier, time_to_token_id)?;
            }
        }
        Ok(())
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
        let tier_groups = parse_tier_map(properties.get(_PROP_TIER_GROUPS).ok_or_else(|| {
            anyhow!(
                "No tier mapping configurated (property \"{}\" missing). Cannot proceed.",
                _PROP_TIER_GROUPS
            )
        })?);
        let params = MapperParams {
            tier_groups,
            force_multi_tok: properties
                .get(_PROP_FORCE_MULTI_TOK)
                .map_or(false, |v| v.trim().eq_ignore_ascii_case("true")),
            skip_audio: properties
                .get(_PROP_SKIP_AUDIO)
                .map_or(false, |v| v.trim().eq_ignore_ascii_case("true")),
            skip_time_annotations: properties
                .get(_PROP_SKIP_TIME_ANNOS)
                .map_or(false, |v| v.trim().eq_ignore_ascii_case("true")),
            audio_extension: properties
                .get(_PROP_AUDIO_EXTENSION)
                .map_or("wav", |ext| ext.as_str()),
        };

        let documents = path_structure(&mut u, input_path, &_FILE_ENDINGS)?;
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

            let mut doc_mapper = DocumentMapper {
                doc_path,
                textgrid,
                reporter: &reporter,
                file_path,
                params: &params,
                number_of_spans: 0,
            };

            doc_mapper.map(&mut u)?;
            reporter.worked(1)?;
        }
        Ok(u)
    }
}
