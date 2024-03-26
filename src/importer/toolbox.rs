use std::{
    collections::{
        btree_map::{Entry, VacantEntry},
        BTreeMap,
    },
    fs,
    path::Path,
};

use graphannis::update::{GraphUpdate, UpdateEvent};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use serde::Deserialize;

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
    StepID,
};

use super::Importer;

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ImportToolBox {
    #[serde(default)]
    layer_map: BTreeMap<String, Vec<String>>,
}

const FILE_EXTENSIONS: [&str; 1] = ["txt"];

impl Importer for ImportToolBox {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, self.file_extensions())?;
        let progress = ProgressReporter::new(tx, step_id.clone(), paths_and_node_names.len())?;
        for (path, doc_node_name) in paths_and_node_names {
            self.map_document(path.as_path(), &doc_node_name, &mut update, &step_id)?;
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

impl ImportToolBox {
    fn map_document(
        &self,
        path: &Path,
        doc_node_name: &str,
        update: &mut GraphUpdate,
        step_id: &StepID,
    ) -> Result<()> {
        let data = fs::read_to_string(path)?;
        let mut pairs =
            ToolboxParser::parse(Rule::data, &data).map_err(|e| AnnattoError::Import {
                reason: format!("Could not parse toolbox file."),
                importer: step_id.module_name.clone(),
                path: path.to_path_buf(),
            })?;
        let next_pair = pairs.next();
        let mut start_id = 1;
        if let Some(pair) = next_pair {
            if pair.as_rule() == Rule::data {
                for annotation_block in pair.into_inner() {
                    if annotation_block.as_rule() == Rule::block {
                        start_id = self.map_annotation_block(
                            update,
                            doc_node_name,
                            annotation_block,
                            start_id,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn map_annotation_block(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        start_id: usize,
    ) -> Result<usize> {
        let mut pass_id = start_id;
        for line in data.into_inner() {
            if line.as_rule() == Rule::line {
                pass_id = self.map_annotation_line(update, doc_node_name, line, pass_id)?;
            }
        }
        Ok(pass_id)
    }

    fn map_annotation_line(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        start_id: usize,
    ) -> Result<usize> {
        let mut layer_name = String::new();
        let mut latest_id = start_id;
        for pair in data.into_inner() {
            match pair.as_rule() {
                Rule::entries => {
                    latest_id =
                        self.map_line_entries(update, doc_node_name, pair, &layer_name, start_id)?;
                }
                Rule::anno_field => {
                    layer_name.push_str(pair.as_str().trim());
                }
                Rule::proc_field => return Ok(latest_id), // TODO make configurable, for now internal markers ("\_...") are not processed
                _ => {}
            }
        }
        Ok(latest_id)
    }

    fn map_line_entries(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        anno_name: &str,
        id: usize,
    ) -> Result<usize> {
        let inner = data.into_inner();
        let mut timeline_id = id;
        for entry in inner {
            match entry.as_rule() {
                Rule::entry => {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: format!("{doc_node_name}#t{timeline_id}"),
                        anno_ns: "".to_string(),
                        anno_name: anno_name.to_string(),
                        anno_value: entry.as_str().to_string(),
                    })?;
                    timeline_id += 1;
                }
                Rule::null => {
                    timeline_id += 1;
                }
                _ => {}
            }
        }
        Ok(timeline_id)
    }
}

#[derive(Parser)]
#[grammar = "importer/toolbox/toolbox.pest"]
struct ToolboxParser;
