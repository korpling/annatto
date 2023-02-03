use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::path::PathBuf;
use std::*;

use crate::models::textgrid::{TextGrid, TextGridItem};
use crate::progress::ProgressReporter;
use crate::util::graphupdate::{add_order_relations, map_audio_source, map_token, path_structure};
use crate::Module;
use anyhow::{anyhow, Result};
use graphannis::update::GraphUpdate;
use ordered_float::OrderedFloat;

use super::Importer;
const _FILE_ENDINGS: [&str; 3] = [".textgrid", ".TextGrid", ".textGrid"];
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

struct DocumentMapper<'a> {
    doc_path: String,
    textgrid: TextGrid,
    reporter: &'a ProgressReporter,
    file_path: PathBuf,
    params: &'a MapperParams<'a>,
}

impl<'a> DocumentMapper<'a> {
    fn map(&self, u: &mut GraphUpdate) -> Result<()> {
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
        let time_to_index = if is_multi_tok {
            let time_to_index = self.map_timeline(u)?;
            time_to_index
        } else {
            BTreeMap::default()
        };

        for (tok_tier_name, dependend_tiers) in self.params.tier_groups.iter() {
            self.map_tier_group(u, tok_tier_name)?;
        }

        todo!()
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
                &format!("tli{}", counter),
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

    fn map_tier_group(&self, u: &mut GraphUpdate, tok_tier_name: &str) -> Result<()> {
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
            for (token_idx, i) in intervals.iter().enumerate() {
                map_token(
                    u,
                    &self.doc_path,
                    &format!("{}{}", tok_tier_name, token_idx),
                    Some(name),
                    &i.text,
                    start_time,
                    end_time,
                    true,
                )?;
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
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(input_path), 2)?;
        let mut u = GraphUpdate::default();
        let tier_groups = parse_tier_map(
            properties
                .get(_PROP_TIER_GROUPS)
                .ok_or_else(|| anyhow!("No tier mapping configurated. Cannot proceed."))?,
        );
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

        for (file_path, doc_path) in path_structure(&mut u, input_path, &_FILE_ENDINGS)? {
            let file_content = std::fs::read_to_string(&file_path)?;
            let textgrid = TextGrid::parse(&file_content)?;

            let doc_mapper = DocumentMapper {
                doc_path,
                textgrid,
                reporter: &reporter,
                file_path,
                params: &params,
            };

            doc_mapper.map(&mut u)?;
        }
        Ok(u)
    }
}

fn parse_tier_map(value: &str) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut tier_map = BTreeMap::new();
    for group in value.split(";") {
        if let Some((owner, objects)) = group.split_once("={") {
            let owner = owner.trim();
            let value: BTreeSet<_> = objects[0..(objects.len() - 2)]
                .split(",")
                .map(|e| e.trim())
                .collect();
            tier_map.insert(owner, value);
        }
    }
    return tier_map;
}
