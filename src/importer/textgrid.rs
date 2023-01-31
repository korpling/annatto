use std::collections::{BTreeSet, BTreeMap};
use std::fs::{read, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::*;
use std::{collections::HashMap, path::PathBuf};

use crate::error::AnnattoError;
use crate::progress::ProgressReporter;
use crate::util::graphupdate::{map_audio_source, path_structure};
use crate::{Module, Result};
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

#[derive(Error, Debug)]
pub enum TextgridImporterError {
    #[error("Header (first line in file) is missing")]
    HeaderMissing,
}

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
        let file = File::read(file_path)?;
        let buffered_file = BufReader::new(file);
        let data = buffered_file.lines();
        if !self.skip_audio {
            // TODO: Check assumption that the audio file is always relative to the actual file
            let audio_path = file_path.with_extension(self.audio_extension);
            if audio_path.exists() {
                map_audio_source(u, &audio_path, corpus_doc_path)?;
            } else {
                self.reporter.info(&format!(
                    "Could not find corresponding audio file {}",
                    audio_path.to_string_lossy()
                ));
            }
        }
        let header = data
            .next()
            .ok_or_else(|| TextgridImporterError::HeaderMissing)??;
        let file_type = header[(header.find("\"") + 1)..header.rfind("\"")];
        let tier_names : BTreeSet<_> = self.tier_groups.iter().flat_map(|(_k,v)| *v).collect();
        // let tiers_and_values = process_data(u, data, tier_names, file_type == _FILE_TYPE_SHORT);
        // let is_multi_tok = self.tier_map.len() > 1 || self.force_multitok;
        // let tok_dict = HashMap::new();
        // if is_multi_tok {
        //     let valid_time_values = sorted(set(chain(starred!(tier_names
        //         .iter()
        //         .map(|tok_name| (t0, t1))
        //         .collect::<Vec<_>>()) /*unsupported*/)));
        //     for i in (0..valid_time_values.len()) {
        //         let (start, end) = valid_time_values[i..(i + 2)];
        //         tok_dict[(start, end)] = map_token(
        //             u,
        //             corpus_doc_path,
        //             (i + 1),
        //             "",
        //             " ",
        //             if skip_time_annotations { None } else { start },
        //             if skip_time_annotations { None } else { end },
        //         );
        //     }
        //     add_order_relations(
        //         u,
        //         sorted(tok_dict.items(), |e| e[0][0])
        //             .iter()
        //             .map(|((s, e), id_)| id_)
        //             .collect::<Vec<_>>(),
        //         "",
        //     );
        // }
        // let mut tc = if is_multi_tok { tok_dict.len() } else { 0 };
        // let mut spc = 0;
        // for (tok_tier, dependent_tiers) in self.tier_map.items() {
        //     let start_times = set();
        //     let end_times = set();
        //     for (start, end, value) in tiers_and_values[tok_tier] {
        //         if !value.strip() {
        //             continue;
        //         }
        //         tok_dict[(start, end, tok_tier)] = map_token(
        //             u,
        //             corpus_doc_path,
        //             tc,
        //             tok_tier,
        //             value,
        //             if skip_time_annotations { None } else { start },
        //             if skip_time_annotations { None } else { end },
        //         );
        //         tc += 1;
        //         if is_multi_tok {
        //             let mut overlapped = tok_dict
        //                 .items()
        //                 .iter()
        //                 .cloned()
        //                 .filter(|&(k, id_)| k.len() == 2 && start <= k[0] && end >= k[1])
        //                 .map(|(k, id_)| id_)
        //                 .collect::<Vec<_>>();
        //             coverage(u, vec![tok_dict[(start, end, tok_tier)]], overlapped);
        //         }
        //         start_times.add(start);
        //         end_times.add(end);
        //     }
        //     let all_tokens = sorted(tok_dict.items(), |e| e[0][0])
        //         .iter()
        //         .cloned()
        //         .filter(|&((_, _, name), id_)| name == tok_tier)
        //         .map(|((_, _, name), id_)| id_)
        //         .collect::<Vec<_>>();
        //     if !all_tokens {
        //         _logger.exception(
        //             "Token tier {tok_tier} does not exist or does not cover any labelled interval.",
        //         );
        //     }
        //     if !is_multi_tok {
        //         add_order_relations(u, all_tokens, "");
        //     }
        //     add_order_relations(u, all_tokens, tok_tier);
        //     let span_dict = HashMap::new();
        //     let ordered_start_times = sorted(start_times);
        //     let ordered_end_times = sorted(end_times);
        //     for tier_name in dependent_tiers {
        //         for (start, end, value) in tiers_and_values[tier_name] {
        //             if !value.strip() {
        //                 continue;
        //             }
        //             if span_dict.iter().all(|&x| x != (start, end)) {
        //                 spc += 1;
        //                 let corrected_start = if start_times.iter().all(|&x| x != start) {
        //                     start_times.iter().min().unwrap()
        //                 } else {
        //                     start
        //                 };
        //                 let corrected_end = if end_times.iter().all(|&x| x != end) {
        //                     end_times.iter().min().unwrap()
        //                 } else {
        //                     end
        //                 };
        //                 if corrected_start == corrected_end {
        //                     let alternative_a = (
        //                         ordered_start_times[(ordered_start_times.index(corrected_start) - 1)],
        //                         corrected_end,
        //                     );
        //                     let alternative_b = (
        //                         corrected_start,
        //                         ordered_end_times[(ordered_end_times.index(corrected_end) + 1)],
        //                     );
        //                     let (corrected_start, corrected_end) = alternative_a.iter().max().unwrap();
        //                 }
        //                 let mut overlapped = tok_dict
        //                     .items()
        //                     .iter()
        //                     .cloned()
        //                     .filter(|&(k, id_)| {
        //                         k.len() == 3
        //                             && k[2] == tok_tier
        //                             && corrected_start <= k[0]
        //                             && corrected_end >= k[1]
        //                     })
        //                     .map(|(k, id_)| id_)
        //                     .collect::<Vec<_>>();
        //                 span_dict[(start, end)] = map_annotation(
        //                     u,
        //                     corpus_doc_path,
        //                     spc,
        //                     tok_tier,
        //                     tier_name,
        //                     value,
        //                     starred!(overlapped), /*unsupported*/
        //                 );
        //                 span_dict[(corrected_start, corrected_end)] = span_dict[(start, end)];
        //             } else {
        //                 u.add_node_label(span_dict[(start, end)], tok_tier, tier_name, value);
        //             }
        //         }
        //     }
        //  }
        todo!()
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
        let tier_groups = parse_tier_map(properties.get(_PROP_TIER_GROUPS).ok_or_else(|| {
            AnnattoError::ReadWorkflowFile(
                "No tier mapping configurated. Cannot proceed.".to_string(),
            )
        })?);

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

// fn process_data<T0, T1, T2, T3, RT>(u: T0, data: T1, tier_names: T2, short: T3) -> RT {
//     let resolver = if short { resolve_short } else { resolve_long };
//     let mut gathered = vec![];
//     let mut size = 0;
//     let tier_data = defaultdict(list);
//     for line in data[9..] {
//         let l = line.strip();
//         if size == 0 {
//             if !short && l.startswith("item [") {
//                 continue;
//             }
//             if gathered.len() < 5 {
//                 gathered.push(resolver(l));
//             } else {
//                 let (clz, name, _, _, size) = gathered;
//                 gathered.clear();
//             }
//         } else {
//             if gathered.len() < 3 {
//                 gathered.push(resolver(l));
//             } else {
//                 tier_data[name].append(tuple(gathered));
//                 gathered.clear();
//                 size -= 1;
//             }
//         }
//     }
//     return tier_data;
// }
// fn resolve_short<T0, RT>(value: T0) -> RT {
//     if value.startswith("\"") {
//         return value[1..-1];
//     } else {
//         if value.iter().any(|&x| x == ".") {
//             return float(value);
//         } else {
//             return i32::from(value);
//         }
//     }
// }
// fn resolve_long<T0, RT>(value: T0) -> RT {
//     let try_dummy = {
//         //unsupported
//         let bare_value = value.split(" = ", 1)[1];
//     };
//     let except!(IndexError) = {
//         //unsupported
//         raise!(ValueError("Could not preprocess line `{value}` correctly")); //unsupported
//     };
//     return resolve_short(bare_value);
// }

fn parse_tier_map(value: &str) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut tier_map = HashMap::new();
    for group in value.split(";") {
        if let Some((owner, objects)) = group.split_once("={") {
            let owner = owner.trim();
            let value: Vec<_> = objects[0..(objects.len() - 2)]
                .split(",")
                .map(|e| e.trim())
                .collect();
            tier_map.insert(owner, value);
        }
    }
    return tier_map;
}
