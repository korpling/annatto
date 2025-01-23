use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use csv::Reader;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{graph::ANNIS_NS, util::split_qname};

use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Importer;
use crate::{
    progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files, StepID,
};

use crate::deserialize::{deserialize_anno_key, deserialize_annotation_component_opt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyLineGroup {
    #[serde(deserialize_with = "deserialize_anno_key")]
    anno: AnnoKey,
    #[serde(deserialize_with = "deserialize_annotation_component_opt", default)]
    component: Option<AnnotationComponent>,
}

/// Import CSV files with token and token annotations.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportTable {
    /// If not empty, skip the first row and use this list as the fully qualified annotation name for each column.
    column_names: Vec<String>,
    /// The provided character defines the column delimiter. The default value is tab.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// delimiter = ";"
    /// ```
    #[serde(default = "default_delimiter")]
    delimiter: char,
    /// The provided character will be used for quoting values. If nothing is provided, all columns will contain bare values. If a character is provided,
    /// all values will be quoted.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// quote_char = "\""
    /// ```
    #[serde(default)]
    quote_char: Option<char>,
    /// If given, treat empty lines as separator for spans of token (e.g.
    /// sentences). You need to configure the name of the annotation to create
    /// (`anno`).
    /// Example:
    /// ```toml
    /// [import.config]
    /// empty_line_group = {anno="csv::sent_id"}
    /// ```
    /// The annotation value will be a sequential number.
    ///
    /// Per default, a span is created, but you can change the `component` e.g. to a one of the type dominance.
    ///
    /// ```toml
    /// [import.config]
    /// empty_line_group = {anno = "csv::sentence, value="S", component = {ctype="Dominance", layer="syntax", name="cat"}}
    /// ```
    ///
    #[serde(default)]
    empty_line_group: Option<EmptyLineGroup>,
}

fn default_delimiter() -> char {
    '\t'
}

impl Default for ImportTable {
    fn default() -> Self {
        Self {
            column_names: Vec::new(),
            quote_char: None,
            delimiter: default_delimiter(),
            empty_line_group: None,
        }
    }
}

const FILE_ENDINGS: [&str; 4] = ["csv", "tsv", "tab", "txt"];

impl Importer for ImportTable {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, self.file_extensions())?;
        let progress =
            ProgressReporter::new(tx.clone(), step_id.clone(), paths_and_node_names.len())?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            self.import_document(&mut update, pathbuf.as_path(), doc_node_name)?;
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_ENDINGS
    }
}
impl ImportTable {
    fn import_document(
        &self,
        update: &mut GraphUpdate,
        document_path: &Path,
        document_node_name: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut reader_builder = csv::ReaderBuilder::new();
        reader_builder
            .delimiter(self.delimiter as u8)
            .quoting(false)
            .trim(csv::Trim::All)
            .flexible(true);
        if let Some(c) = &self.quote_char {
            reader_builder.quoting(true).quote(*c as u8);
        }
        if self.column_names.is_empty() {
            reader_builder.has_headers(true);
        } else {
            reader_builder.has_headers(false);
        }
        let reader = reader_builder.from_path(document_path)?;

        self.map_token(update, &document_node_name, reader)?;

        if let Some(empty_line_group) = &self.empty_line_group {
            // Go trough the file and find empty lines
            let f = File::open(document_path)?;
            let buffered_reader = BufReader::new(f);

            let mut empty_line_nr = 1;
            let mut group_start_token: u64 = 1;
            let mut next_token_idx = 1;
            for line in buffered_reader.lines() {
                let line = line?;

                if line.trim_ascii().is_empty() {
                    self.map_span(
                        update,
                        group_start_token,
                        next_token_idx,
                        empty_line_group,
                        &document_node_name,
                        empty_line_nr.to_string(),
                    )?;
                    empty_line_nr += 1;
                    group_start_token = next_token_idx;
                } else {
                    // Token are only added for non-empty lines
                    next_token_idx += 1;
                }
            }
            if next_token_idx > group_start_token {
                // Map the last group as well
                self.map_span(
                    update,
                    group_start_token,
                    next_token_idx,
                    empty_line_group,
                    &document_node_name,
                    empty_line_nr.to_string(),
                )?;
            }
        }

        Ok(())
    }

    fn map_span(
        &self,
        update: &mut GraphUpdate,
        group_start_token: u64,
        next_token_idx: u64,
        empty_line_group: &EmptyLineGroup,
        document_node_name: &str,
        value: String,
    ) -> anyhow::Result<()> {
        let group_span_name = format!(
            "{document_node_name}#group_span_{group_start_token}_{}",
            next_token_idx - 1
        );

        update.add_event(UpdateEvent::AddNode {
            node_name: group_span_name.clone(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: group_span_name.clone(),
            anno_ns: empty_line_group.anno.ns.to_string(),
            anno_name: empty_line_group.anno.name.to_string(),
            anno_value: value,
        })?;
        update.add_event(UpdateEvent::AddEdge {
            source_node: group_span_name.clone(),
            target_node: document_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        // Add spanning relations for all covered token
        for t in group_start_token..next_token_idx {
            if let Some(c) = &empty_line_group.component {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: group_span_name.clone(),
                    target_node: format!("{document_node_name}#t{t}"),
                    layer: c.layer.to_string(),
                    component_type: c.get_type().to_string(),
                    component_name: c.name.to_string(),
                })?;
            } else {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: group_span_name.clone(),
                    target_node: format!("{document_node_name}#t{t}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        Ok(())
    }

    fn map_token<R>(
        &self,
        update: &mut GraphUpdate,
        document_node_name: &str,
        mut reader: Reader<R>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        R: std::io::Read,
    {
        let column_names: Vec<_> = if reader.has_headers() {
            reader.headers()?.iter().map(|h| h.to_string()).collect()
        } else {
            self.column_names.clone()
        };

        let mut token_idx = 1;

        for record in reader.records() {
            let record = record?;

            // Add node for token
            let node_name = format!("{document_node_name}#t{token_idx}");
            update.add_event(UpdateEvent::AddNode {
                node_name: node_name.clone(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.clone(),
                target_node: document_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if token_idx > 0 {
                let last_token_node_name = format!("{document_node_name}#t{}", token_idx - 1);
                update.add_event(UpdateEvent::AddEdge {
                    source_node: last_token_node_name.clone(),
                    target_node: node_name.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }

            // Add all columns as token annotations
            for (i, name) in column_names.iter().enumerate() {
                if let Some(val) = record.get(i) {
                    let (ns, name) = split_qname(name);
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.clone(),
                        anno_ns: ns.unwrap_or_default().to_string(),
                        anno_name: name.to_string(),
                        anno_value: val.to_string(),
                    })?;
                }
            }
            token_idx += 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
