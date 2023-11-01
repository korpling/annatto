use std::{collections::BTreeMap, io::Read, path::Path};

use crate::{
    progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files, Module, StepID,
};

use super::Importer;
use encoding_rs::Encoding;
use encoding_rs_io::DecodeReaderBytesBuilder;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use pest::{iterators::Pairs, Parser};
use pest_derive::Parser;
use serde::Deserialize;

const FILE_ENDINGS: [&str; 5] = ["treetagger", "tab", "tt", "txt", "xml"];

pub const MODULE_NAME: &str = "import_treetagger";

#[derive(Parser)]
#[grammar = "importer/treetagger/treetagger.pest"]
pub struct TreeTaggerParser;

enum Column {
    Token,
    Anno(String),
}

impl From<String> for Column {
    fn from(value: String) -> Self {
        if value == "tok" {
            Self::Token
        } else {
            Self::Anno(value)
        }
    }
}

struct MapperParams {
    column_names: Vec<Column>,
}

struct DocumentMapper<'a> {
    doc_path: String,
    text_node_name: String,
    last_token_id: Option<String>,
    number_of_token: usize,
    tag_stack: BTreeMap<String, Vec<String>>,
    params: &'a MapperParams,
}

impl<'a> DocumentMapper<'a> {
    fn map(&mut self, u: &mut GraphUpdate, mut tt: Pairs<'a, Rule>) -> anyhow::Result<()> {
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

        if let Some(tt) = tt.next() {
            if tt.as_rule() == Rule::treetagger {
                let tt = tt.into_inner();
                self.map_tt_rule(u, tt)?;
            }
        }
        Ok(())
    }

    fn map_tt_rule(&mut self, u: &mut GraphUpdate, tt: Pairs<'a, Rule>) -> anyhow::Result<()> {
        for line in tt {
            match line.as_rule() {
                Rule::token_line => {
                    let token_line = line.into_inner();
                    self.consume_token_line(u, token_line)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn consume_token_line(
        &mut self,
        u: &mut GraphUpdate,
        mut token_line: Pairs<'a, Rule>,
    ) -> anyhow::Result<()> {
        // Create a token node for this column
        let id = self.number_of_token + 1;
        let tok_id = format!("{}#t{id}", self.doc_path);
        u.add_event(UpdateEvent::AddNode {
            node_name: tok_id.clone(),
            node_type: "node".to_string(),
        })?;

        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: tok_id.clone(),
            target_node: self.text_node_name.clone(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;

        if let Some(last_token_id) = &self.last_token_id {
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_id.clone(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok-whitespace-before".to_string(),
                anno_value: " ".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: last_token_id.clone(),
                target_node: tok_id.clone(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
        self.number_of_token += 1;
        self.last_token_id = Some(tok_id.clone());

        for column_def in &self.params.column_names {
            if let Some(column_value) = token_line.next() {
                if column_value.as_rule() == Rule::column_value {
                    match column_def {
                        Column::Token => {
                            u.add_event(UpdateEvent::AddNodeLabel {
                                node_name: tok_id.to_string(),
                                anno_ns: ANNIS_NS.to_string(),
                                anno_name: "tok".to_string(),
                                anno_value: column_value.as_str().to_string(),
                            })?;
                        }
                        Column::Anno(anno_name) => {
                            u.add_event(UpdateEvent::AddNodeLabel {
                                node_name: tok_id.to_string(),
                                anno_ns: DEFAULT_NS.to_string(),
                                anno_name: anno_name.clone(),
                                anno_value: column_value.as_str().to_string(),
                            })?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Importer for the file format used by the TreeTagger.
#[derive(Default, Deserialize)]
#[serde(default)]
pub struct TreeTaggerImporter {
    column_names: Vec<String>,
    /// The encoding to use when for the input files. Defaults to UTF-8.
    encoding: Option<String>,
}

impl Module for TreeTaggerImporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for TreeTaggerImporter {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();

        let documents = import_corpus_graph_from_files(&mut u, input_path, &FILE_ENDINGS)?;

        let reporter = ProgressReporter::new(tx, step_id, documents.len())?;

        let mut params = MapperParams {
            column_names: self
                .column_names
                .iter()
                .map(|c| Column::from(c.clone()))
                .collect(),
        };
        // Set a default column configuration when nothing configured
        if params.column_names.is_empty() {
            params.column_names.push(Column::Token);
            params.column_names.push(Column::Anno("pos".into()));
            params.column_names.push(Column::Anno("lemma".into()));
        }

        let decoder_builder = if let Some(encoding) = &self.encoding {
            DecodeReaderBytesBuilder::new()
                .encoding(Encoding::for_label(encoding.as_bytes()))
                .clone()
        } else {
            DecodeReaderBytesBuilder::new()
        };

        for (file_path, doc_path) in documents {
            reporter.info(&format!("Processing {}", &file_path.to_string_lossy()))?;

            let f = std::fs::File::open(&file_path)?;
            let mut file_content = String::new();

            decoder_builder
                .build(&f)
                .read_to_string(&mut file_content)?;

            let tt: Pairs<Rule> = TreeTaggerParser::parse(Rule::treetagger, &file_content)?;

            let text_node_name = format!("{}#text", &doc_path);

            let mut doc_mapper = DocumentMapper {
                doc_path,
                text_node_name,
                params: &params,
                last_token_id: None,
                number_of_token: 0,
                tag_stack: BTreeMap::new(),
            };

            doc_mapper.map(&mut u, tt)?;
            reporter.worked(1)?;
        }
        Ok(u)
    }
}

#[cfg(test)]
mod tests;
