use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Annotation},
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
    StepID,
};

use super::Importer;

/// Import annotations provided in the fieldlinguist's toolbox text format.
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportToolBox {
    /// This map links annotation layers to ordered (token) layers.
    #[serde(default)]
    order: BTreeSet<String>,
    /// The annotation names named here are considered single-valued per line. Space values
    /// are not considered delimiters, but part of the annotation value. Such annotations
    /// rely on the existence of the target nodes, i. e. annotation lines without any other
    /// non-spanning annotation in the block will be dropped.
    #[serde(default)]
    span: BTreeSet<String>,
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
                reason: format!("Failed to parse: {}", e),
                importer: step_id.module_name.clone(),
                path: path.to_path_buf(),
            })?;
        let next_pair = pairs.next();
        let mut start_id = 1;
        let mut order_ends = BTreeMap::default();
        if let Some(pair) = next_pair {
            if pair.as_rule() == Rule::data {
                for annotation_block in pair.into_inner() {
                    if annotation_block.as_rule() == Rule::block {
                        start_id = self.map_annotation_block(
                            update,
                            doc_node_name,
                            annotation_block,
                            start_id,
                            &mut order_ends,
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
        order_ends: &mut BTreeMap<String, String>,
    ) -> Result<usize> {
        let mut end_ids = BTreeSet::default();
        end_ids.insert(start_id); // to guarantee an option of a maximal value
        let lines = data.into_inner();
        let mut block_annos = Vec::with_capacity(lines.len());
        for line in lines {
            if line.as_rule() == Rule::line {
                let (end_id, block_anno) =
                    self.map_annotation_line(update, doc_node_name, line, start_id, order_ends)?;
                end_ids.insert(end_id);
                if let Some(anno) = block_anno {
                    block_annos.push(anno);
                }
            }
        }
        let max_end = end_ids.into_iter().max().unwrap();
        for anno in block_annos {
            let node_name = format!("{doc_node_name}#n{start_id}-{max_end}");
            let tokens = (start_id..max_end)
                .into_iter()
                .map(|i| format!("{doc_node_name}#t{i}"))
                .collect_vec();
            self.annotate_tokens(
                update,
                doc_node_name,
                node_name,
                &tokens,
                &anno.key.ns,
                &anno.key.name,
                &anno.val,
                order_ends,
            )?;
        }
        Ok(max_end)
    }

    fn map_annotation_line(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        start_id: usize,
        order_ends: &mut BTreeMap<String, String>,
    ) -> Result<(usize, Option<Annotation>)> {
        let mut layer_name = String::new();
        let mut end_id = start_id;
        let mut span_anno = None;
        for pair in data.into_inner() {
            match pair.as_rule() {
                Rule::entries => {
                    let (final_id, block_anno) = self.map_line_entries(
                        update,
                        doc_node_name,
                        pair,
                        &layer_name,
                        start_id,
                        order_ends,
                    )?;
                    if let Some(anno_val) = block_anno {
                        span_anno = Some(Annotation {
                            key: AnnoKey {
                                ns: "".into(),
                                name: layer_name.into(),
                            },
                            val: anno_val.into(),
                        });
                    }
                    end_id = final_id;
                    return Ok((end_id, span_anno));
                }
                Rule::anno_field => {
                    layer_name.push_str(pair.as_str().trim());
                }
                Rule::proc_field => return Ok((end_id, None)), // TODO make configurable, for now internal markers ("\_...") are not processed
                _ => {}
            }
        }
        Ok((end_id, span_anno)) // if this is ever executed, something went wrong
    }

    fn map_line_entries(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        anno_name: &str,
        id: usize,
        order_ends: &mut BTreeMap<String, String>,
    ) -> Result<(usize, Option<String>)> {
        let inner = data.into_inner();
        // Build nodes
        // FIXME this will lead to multiple creations of the same nodes and edges, which is not problematic, but slows things down
        build_ordered_nodes(update, doc_node_name, id, inner.len())?;
        // annotate nodes
        let mut timeline_id = id;
        let mut join_list = Vec::new();
        let build_joint = self.span.contains(anno_name);
        for entry_or_space in inner {
            match entry_or_space.as_rule() {
                Rule::entry => {
                    if build_joint {
                        join_list.push(entry_or_space.as_str());
                    } else {
                        self.annotate_tokens(
                            update,
                            doc_node_name,
                            format!("{doc_node_name}#n{timeline_id}"),
                            &[format!("{doc_node_name}#t{timeline_id}")],
                            "",
                            anno_name,
                            entry_or_space.as_str(),
                            order_ends,
                        )?;
                        timeline_id += 1;
                    }
                }
                Rule::spaces => {
                    if build_joint {
                        join_list.push(entry_or_space.as_str());
                    }
                }
                Rule::null => {
                    timeline_id += 1;
                }
                _ => {}
            }
        }
        let block_annotation = if build_joint && !join_list.is_empty() {
            Some(join_list.join(""))
        } else {
            None
        };
        Ok((timeline_id, block_annotation))
    }

    fn annotate_tokens(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        node_name: String,
        tokens: &[String],
        anno_ns: &str,
        anno_name: &str,
        anno_value: &str,
        order_ends: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        update.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: anno_ns.to_string(),
            anno_name: anno_name.to_string(),
            anno_value: anno_value.to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        for token_name in tokens {
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: token_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
        }
        update.add_event(UpdateEvent::AddEdge {
            source_node: node_name.to_string(),
            target_node: doc_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        if self.order.contains(anno_name) {
            if let Entry::Occupied(e) = order_ends.entry(anno_name.to_string()) {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: e.get().to_string(),
                    target_node: node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: anno_name.to_string(),
                })?;
            }
            order_ends.insert(anno_name.to_string(), node_name);
        }
        Ok(())
    }
}

fn build_ordered_nodes(
    update: &mut GraphUpdate,
    doc_node_name: &str,
    start: usize,
    n: usize,
) -> Result<()> {
    (start..(start + n))
        .into_iter()
        .try_for_each(|i| {
            let node_name = format!("{doc_node_name}#t{i}");
            update
                .add_event(UpdateEvent::AddNode {
                    node_name: node_name.to_string(),
                    node_type: "node".to_string(),
                })
                .and_then(|_| {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: " ".to_string(),
                    })
                })
                .and_then(|_| {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "layer".to_string(),
                        anno_value: "default_layer".to_string(),
                    })
                })
                .and_then(|_| {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: node_name.to_string(),
                        target_node: doc_node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })
                })
                .and_then(|_| {
                    if i > 1 {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: format!("{doc_node_name}#t{}", i - 1),
                            target_node: node_name,
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                })
        })
        .map_err(|e| AnnattoError::GraphAnnisCore(e))
}

#[derive(Parser)]
#[grammar = "importer/toolbox/toolbox.pest"]
struct ToolboxParser;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use insta::assert_snapshot;

    use crate::importer::toolbox::ImportToolBox;

    #[test]
    fn core_functionality() {
        let toml_str = r#"""
        order = ["txt"]
        span = ["ref", "Subref"]
        """#;
        let imp: Result<ImportToolBox, _> = toml::from_str(toml_str);
        assert!(imp.is_ok());
        let importer = imp.unwrap();
        let graphml_is = crate::test_util::import_as_graphml_string(
            importer,
            Path::new("tests/data/import/toolbox/"),
            None,
        );
        assert!(graphml_is.is_ok());
        assert_snapshot!(graphml_is.unwrap());
    }
}
