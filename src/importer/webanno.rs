use std::{fs, path::Path};

use anyhow::{anyhow, bail};
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files};

use super::Importer;

/// Import WebAnno TSV format.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default)]
pub struct ImportWebAnnoTSV {}

const FILE_EXTENSIONS: [&str; 2] = ["tsv", "csv"];

impl Importer for ImportWebAnnoTSV {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, self.file_extensions())?;
        let progress =
            ProgressReporter::new(tx.clone(), step_id.clone(), paths_and_node_names.len())?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            self.import_document(pathbuf.as_path(), doc_node_name, &mut update)?;
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

impl ImportWebAnnoTSV {
    fn import_document(
        &self,
        path: &Path,
        doc_node_name: String,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        let data = fs::read_to_string(path)?;
        let mut parse_data = WebAnnoTSVParser::parse(Rule::data, &data)?
            .next()
            .ok_or(anyhow!("Could not parse data"))?
            .into_inner();
        let header = parse_data.next();
        let column_spec = header
            .ok_or(anyhow!("Could not retrieve header"))
            .map(Self::consume_header)??;
        if let Some(body) = parse_data.next() {
            Self::process_body(body, doc_node_name, &column_spec, update)?;
        } else {
            bail!("Missing body in document {doc_node_name}");
        }
        Ok(())
    }

    fn process_body(
        data: Pair<Rule>,
        doc_node_name: String,
        columns: &[Option<AnnoKey>],
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        let mut ordering_node = None;
        for sentence in data.into_inner() {
            ordering_node =
                Self::map_sentence(sentence, &doc_node_name, columns, update, ordering_node)?;
        }
        Ok(())
    }

    fn map_sentence(
        sentence: Pair<Rule>,
        doc_node_name: &str,
        columns: &[Option<AnnoKey>],
        update: &mut GraphUpdate,
        mut previous_token: Option<String>,
    ) -> Result<Option<String>, anyhow::Error> {
        let sentence_node_name = format!("{doc_node_name}#sentence_line{}", sentence.line_col().0);
        update.add_event(UpdateEvent::AddNode {
            node_name: sentence_node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddEdge {
            source_node: sentence_node_name.to_string(),
            target_node: doc_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for member in sentence.into_inner() {
            match member.as_rule() {
                Rule::sentence_meta => {
                    let mut inner = member.into_inner();
                    let anno_name = inner.next();
                    let anno_value = inner.next();
                    if let (Some(name), Some(value)) = (anno_name, anno_value) {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: sentence_node_name.to_string(),
                            anno_ns: "".to_string(),
                            anno_name: name.as_str().trim().to_string(),
                            anno_value: value.as_str().trim().to_string(),
                        })?;
                    }
                }
                Rule::token => {
                    let tok_name = Self::map_token(member, doc_node_name, columns, update)?;
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: sentence_node_name.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string(),
                    })?;
                    if let Some(prev_tok_name) = previous_token {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: prev_tok_name,
                            target_node: tok_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    previous_token = Some(tok_name);
                }
                _ => {}
            }
        }
        Ok(previous_token)
    }

    fn map_token(
        token: Pair<Rule>,
        doc_node_name: &str,
        columns: &[Option<AnnoKey>],
        update: &mut GraphUpdate,
    ) -> Result<String, anyhow::Error> {
        let mut members = token.into_inner();
        let id = members.next().ok_or(anyhow!("Token has no id"))?.as_str();
        let token_name = format!("{doc_node_name}#{id}");
        update.add_event(UpdateEvent::AddNode {
            node_name: token_name.to_string(),
            node_type: "node".to_string(),
        })?;
        members
            .next()
            .ok_or(anyhow!("No character span for token {}", id))?;
        let form = members
            .next()
            .ok_or(anyhow!(
                "No form for token {id} in document {doc_node_name}"
            ))?
            .as_str();
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: token_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: form.to_string(),
        })?;
        for (anno_key_opt, column_value) in columns.iter().zip(members) {
            if let Some(key) = anno_key_opt {
                if !matches!(column_value.as_rule(), Rule::anno_value) {
                    continue;
                }
                let value = column_value.as_str().trim();
                if key.name.is_empty() {
                    // annotation names are coded in the value as feature string
                    for single_anno in column_value.as_str().split("|") {
                        if let Some((k, v)) = single_anno.split_once("=") {
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: token_name.to_string(),
                                anno_ns: key.ns.to_string(),
                                anno_name: k.to_string(),
                                anno_value: v.to_string(),
                            })?;
                        }
                    }
                } else {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: token_name.to_string(),
                        anno_ns: key.ns.to_string(),
                        anno_name: key.name.to_string(),
                        anno_value: value.to_string(),
                    })?;
                }
            }
        }
        Ok(token_name.to_string())
    }

    fn consume_header(data: Pair<Rule>) -> Result<Vec<Option<AnnoKey>>, anyhow::Error> {
        let mut columns = Vec::new();
        for entry in data.into_inner() {
            if matches!(entry.as_rule(), Rule::column) {
                columns.extend(
                    entry
                        .into_inner()
                        .next_back()
                        .ok_or(anyhow!("No anno spec!"))
                        .map(Self::map_column_definition)??,
                );
            }
        }
        Ok(columns)
    }

    fn map_column_definition(data: Pair<Rule>) -> Result<Vec<Option<AnnoKey>>, anyhow::Error> {
        let rule = data.as_rule();
        let mut entries = data.into_inner().map(|p| p.as_str()).collect_vec();
        let (namespace, annotations) = match rule {
            Rule::edge_annotation | Rule::node_annotation => {
                // TODO future work -> properly import edge annotations as edge annotations
                (entries.remove(0).split(".").last(), entries)
            }
            _ => {
                bail!("Illegal mapping rule.")
            }
        };
        let column_keys = if let Some(index) = annotations.iter().position(|s| *s == "value") {
            let mut v = vec![None; annotations.len()];
            v[index] = Some(AnnoKey {
                ns: namespace.unwrap_or_default().into(),
                name: "".into(),
            });
            v
        } else {
            annotations
                .into_iter()
                .map(|s| {
                    Some(AnnoKey {
                        ns: namespace.unwrap_or_default().into(),
                        name: s.into(),
                    })
                })
                .collect_vec()
        };
        Ok(column_keys)
    }
}

#[derive(Parser)]
#[grammar = "importer/webanno/webannotsv.pest"]
struct WebAnnoTSVParser;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        core::update_graph_silent, exporter::graphml::GraphMLExporter, importer::Importer,
        test_util::export_to_string,
    };

    use super::ImportWebAnnoTSV;

    #[test]
    fn default() {
        let import_path = Path::new("tests/data/import/webanno/tsv/");
        let importer: ImportWebAnnoTSV = toml::from_str("").unwrap();
        let u = importer.import_corpus(
            import_path,
            crate::StepID {
                module_name: "test_webanno".to_string(),
                path: Some(import_path.to_path_buf()),
            },
            None,
        );
        assert!(u.is_ok(), "Err: {:?}", u.err());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(update_graph_silent(&mut graph, &mut update).is_ok());
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }
}
