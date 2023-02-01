use core::num;
use pest::iterators::Pairs;
use pest::{Parser, RuleType};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::path::Path;
use std::path::PathBuf;
use std::*;

use crate::progress::ProgressReporter;
use crate::util::graphupdate::{map_audio_source, path_structure};
use crate::Module;
use anyhow::{anyhow, Result};
use graphannis::update::GraphUpdate;

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
/// There are several variants of this format and we don't have a formal parser
/// yet. Thus, the importer should extract the correct information from valid
/// files. But if the file is invalid, there is no guarantee that the error is
/// catched and incomplete information might be extracted.
///
/// See the [Praat
/// Documentation](https://www.fon.hum.uva.nl/praat/manual/TextGrid_file_formats.html)
/// for more information on the format(s) itself.
pub struct TextgridImporter {}

impl Module for TextgridImporter {
    fn module_name(&self) -> &str {
        "TextgridImporter"
    }
}

#[derive(Parser)]
#[grammar = "importer/textgrid.pest"]
pub struct OoTextfileParser;

struct TextgridMapper<'a> {
    reporter: ProgressReporter,
    input_path: PathBuf,
    tier_groups: BTreeMap<&'a str, BTreeSet<&'a str>>,
    force_multi_tok: bool,
    audio_extension: &'a str,
    skip_audio: bool,
    skip_time_annotations: bool,
}

struct TextGridHeader {
    xmin: f64,
    xmax: f64,
    number_items: u64,
}

impl<'a> TextgridMapper<'a> {
    fn map_document(
        &'a self,
        u: &mut GraphUpdate,
        file_path: &Path,
        corpus_doc_path: &str,
    ) -> Result<()> {
        let file_content = std::fs::read_to_string(file_path)?;
        let textgrid = OoTextfileParser::parse(Rule::textgrid, &file_content)?
            .next()
            .ok_or_else(|| anyhow!("No textgrid in file"))?;
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
        // The text grid is a flat sequence of numbers, texts or flags.
        let mut items = textgrid.into_inner();

        // Consume and the items for the document
        let header = self.consume_document_items(&mut items)?;

        todo!()
    }

    fn consume_document_items(&'a self, items: &mut Pairs<'a, Rule>) -> Result<TextGridHeader> {
        let xmin = items
            .next()
            .ok_or_else(|| anyhow!("Missing xmin field for document"))?;

        let xmax = items
            .next()
            .ok_or_else(|| anyhow!("Missing xmax field for document"))?;

        let mut number_items = 0;

        // Check that this document has a tier
        if let Some(tier_flag) = items.next() {
            if tier_flag.as_rule() == Rule::flag && tier_flag.as_str() == "exists" {
                // Get the number of items
                let size = items
                    .next()
                    .ok_or_else(|| anyhow!("Missing size field for document"))?;
                if size.as_rule() == Rule::number {
                    number_items = size.as_str().parse::<u64>()?;
                }
            }
        }

        // No tier has been detected
        let header = TextGridHeader {
            xmin: xmin.as_str().parse::<f64>()?,
            xmax: xmax.as_str().parse::<f64>()?,
            number_items,
        };
        Ok(header)
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

#[cfg(test)]
mod tests;
