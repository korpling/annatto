use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::path::PathBuf;
use std::*;

use crate::models::textgrid::{TextGrid, TextGridItem};
use crate::progress::ProgressReporter;
use crate::util::graphupdate::{map_audio_source, map_token, path_structure, add_order_relations};
use crate::Module;
use anyhow::{anyhow, Result};
use graphannis::update::GraphUpdate;
use graphannis::Graph;
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

struct TextgridMapper<'a> {
    reporter: ProgressReporter,
    input_path: PathBuf,
    tier_groups: BTreeMap<&'a str, BTreeSet<&'a str>>,
    force_multi_tok: bool,
    audio_extension: &'a str,
    skip_audio: bool,
    skip_time_annotations: bool,
}

impl<'a> TextgridMapper<'a> {
    fn map_document(
        &'a self,
        u: &mut GraphUpdate,
        file_path: &Path,
        corpus_doc_path: &str,
    ) -> Result<()> {
        let file_content = std::fs::read_to_string(file_path)?;

        let textgrid = TextGrid::parse(&file_content)?;

        if !self.skip_audio {
            // TODO: Check assumption that the audio file is always relative to the actual file
            let audio_path = file_path.with_extension(self.audio_extension);
            if audio_path.exists() {
                map_audio_source(u, &audio_path, corpus_doc_path)?;
            } else {
                self.reporter.info(&format!(
                    "Could not find corresponding audio file {}",
                    audio_path.to_string_lossy()
                ))?;
            }
        }

        let is_multi_tok = self.tier_groups.len() > 1 || self.force_multi_tok;
        if is_multi_tok {
            let time_to_index = self.map_timeline(u, &textgrid, corpus_doc_path)?;
        }

        todo!()
    }

    fn map_timeline(
        &self,
        u: &mut GraphUpdate,
        textgrid: &TextGrid,
        corpus_doc_path: &str,
    ) -> Result<BTreeMap<OrderedFloat<f64>, String>> {
        // Collect all points of time based on the intervals and points.
        // Sort them by using a sorted set.
        let mut existing_points_of_times: BTreeSet<OrderedFloat<f64>> = BTreeSet::default();
        for tier in textgrid.items.iter() {
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
            let current_token_time = if self.skip_time_annotations {
                None
            } else {
                Some(pot.0)
            };
            let next_token_time = if self.skip_time_annotations {
                None
            } else {
                it.peek().map(|t| t.0)
            };
            
            let tli_id = map_token(
                u,
                corpus_doc_path,
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
        let mapper = TextgridMapper {
            reporter,
            input_path: input_path.to_path_buf(),
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

        for (path, internal_path) in path_structure(&mut u, input_path, &_FILE_ENDINGS, true)? {
            mapper.map_document(&mut u, &path, &internal_path)?;
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
