use std::env::temp_dir;

use csv::ReaderBuilder;
use graphannis::{
    corpusstorage::{QueryLanguage, SearchQuery},
    AnnotationGraph, CorpusStorage,
};
use tempfile::tempdir_in;

use crate::{error::AnnattoError, Manipulator, Module};

pub const MODULE_NAME: &str = "check";
const PROP_CONFIG_PATH: &str = "config.path";
const CONFIG_FILE_ENTRY_SEP: u8 = b'\t';

#[derive(Default)]
pub struct Check {}

impl Module for Check {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

fn read_config_file(path: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(CONFIG_FILE_ENTRY_SEP)
        .from_path(path)?;
    let mut checks = Vec::new();
    for (line_index, entry) in reader.records().into_iter().enumerate() {
        let record = entry?;
        if record.len() != 2 {
            let message = format!(
                "Entry in line {} has invalid length ({} columns instead of 2).",
                line_index,
                record.len()
            );
            let err = AnnattoError::Manipulator {
                reason: message,
                manipulator: MODULE_NAME.to_string(),
            };
            return Err(Box::new(err));
        }
        checks.push((
            record.get(0).unwrap().trim().to_string(),
            record.get(1).unwrap().trim().to_string(),
        ))
    }
    Ok(checks)
}

fn run_checks(
    graph: &mut AnnotationGraph,
    checks_and_results: Vec<(String, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fails = Vec::new();
    let corpus_name = "current";
    let tmp_dir = tempdir_in(temp_dir())?;
    graph.save_to(&tmp_dir.path().join(corpus_name))?;
    let cs = CorpusStorage::with_auto_cache_size(tmp_dir.path(), true)?;
    for (query_s, expected_result) in checks_and_results {
        let result = run_query(&cs, &query_s[..]);
        if let Ok(n) = result {
            let passes = match expected_result.parse::<u64>() {
                Ok(number) => number == n,
                Err(_) => {
                    match &expected_result[..] {
                        "*" => n.ge(&0),
                        "+" => n.ge(&1),
                        "?" => n.ge(&0) && n.le(&1),
                        _ => {
                            // interpret numeric digit as query as well
                            let second_result = run_query(&cs, &expected_result);
                            if let Ok(second_result) = second_result {
                                second_result == n
                            } else {
                                false
                            }
                        }
                    }
                }
            };
            if !passes {
                fails.push(query_s.to_string());
            }
        } else {
            fails.push(format!(
                "Could not be processed: {},{}",
                query_s, &expected_result
            ));
        }
    }
    if fails.is_empty() {
        Ok(())
    } else {
        Err(Box::new(AnnattoError::ChecksFailed {
            failed_checks: fails.join("\n"),
        }))
    }
}

fn run_query(storage: &CorpusStorage, query_s: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let query = SearchQuery {
        corpus_names: &["current"],
        query: query_s,
        query_language: QueryLanguage::AQL,
        timeout: None,
    };
    let c = storage.count(query)?;
    Ok(c)
}

impl Manipulator for Check {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config_path = match properties.get(&PROP_CONFIG_PATH.to_string()) {
            None => {
                return Err(Box::new(AnnattoError::Manipulator {
                    reason: "No test file path provided".to_string(),
                    manipulator: self.module_name().to_string(),
                }))
            }
            Some(path_spec) => &path_spec[..],
        };
        let checks = read_config_file(config_path)?;
        run_checks(graph, checks)
    }
}
