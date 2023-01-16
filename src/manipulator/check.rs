use std::path::Path;

use csv::{Reader, ReaderBuilder};
use graphannis::{AnnotationGraph, corpusstorage::{ResultOrder, SearchQuery, QueryLanguage}, CorpusStorage};

use crate::{Manipulator, Module, error::AnnattoError, workflow::StatusMessage};

pub const MODULE_NAME: &str = "check";
const PROP_CONFIG_PATH: &str = "config.path";
const CONFIG_FILE_ENTRY_SEP: char = ',';

struct Check {}

impl Default for Check {
    fn default() -> Self {
        Check {}
    }
}

impl Module for Check {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

fn read_config_file<'a>(path: &'a str) -> Result<Vec<(&'a str, &'a str)>, Box<dyn std::error::Error>> {
    let reader = ReaderBuilder::new().delimiter(CONFIG_FILE_ENTRY_SEP as u8).from_path(config_path)?;
    let mut checks = Vec::new();
        for (line_index, entry) in reader.records().into_iter().enumerate() {
            let record = entry?;
            if record.len() != 2 {
                let message_s = format!("Entry in line {} has invalid length ({} columns instead of 2).", line_index, record.len());
                if let Some(sender) = &tx {
                    let message = StatusMessage::Failed(message_s);
                    sender.send(message)?;
                } else {
                    let err = AnnattoError::CSV(message_s);
                    return Box::new(Err(err));
                }
            }
            checks.push((record.get(0).unwrap().trim(), record.get(1).unwrap().trim()))
        }
    Ok(checks)
}

fn run_checks(graph: &AnnotationGraph, checks_and_results: Vec<(&str, &str)>) -> Result<(), Box<dyn std::error::Error>> {
    let mut fails = Vec::new();
    let corpus_name = "current";
    let tmp_dir = tempdir_in(temp_dir())?;        
    graph.save_to(&tmp_dir.path().join(corpus_name))?;
    let cs = CorpusStorage::with_auto_cache_size(&tmp_dir.path(), true)?;        
    for (query_s, expected_result) in checks_and_results {
        let query = SearchQuery {
            corpus_names: &[corpus_name],
            query: query_s,
            query_language: QueryLanguage::AQL,
            timeout: None
        };
        let result = cs.find(query, 0, None, ResultOrder::Normal);        
        if let Ok(matches) = result {
            let l = matches.len();
            let passes = match expected_result.parse::<usize>() {
                Ok(number) => {
                    number == l
                },
                Err => {
                    match expected_result {
                        "*" => l >= 0,
                        "+" => l >= 1,
                        "?" => 0 <= l <= 1
                    }
                }
            };
            if !passes {
                fails.push(query_s.to_string());
            }
        } else {
            fails.push(format!("Could not be processed: {}", query_s));
        }
    }
    if fails.is_empty() {
        Ok(())
    }
    Err(Box::new(AnnattoError::ChecksFailed { checks: fails }))
}

impl Manipulator for Check {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config_path = match properties.get(&PROP_CONFIG_PATH.to_string()) {
            None => return Err(Box::new(AnnattoError::Manipulator { reason: "No test file path provided".to_string(), manipulator: self.module_name().to_string() })),
            Some(path_spec) => &path_spec[..]
        };
        let checks = read_config_file(config_path)?;
        
        Ok(())
    }
}