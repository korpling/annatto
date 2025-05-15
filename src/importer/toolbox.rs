use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Annotation},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files,
    StepID,
};

use super::Importer;

/// Import annotations provided in the fieldlinguist's toolbox text format.
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImportToolBox {
    /// This attribute sets the annotation layer, that other annotations will point to.
    /// This needs to be set to avoid an invalid model.
    target: String,
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
        let mut ordering = BTreeMap::default();
        if let Some(pair) = next_pair {
            if pair.as_rule() == Rule::data {
                for annotation_block in pair.into_inner() {
                    if annotation_block.as_rule() == Rule::block {
                        start_id = self.map_annotation_block(
                            update,
                            doc_node_name,
                            annotation_block,
                            start_id,
                            &mut ordering,
                        )?;
                    }
                }
            }
        }
        for (ordering_name, node_specs) in ordering {
            let sorted_nodes = node_specs
                .into_iter()
                .sorted_by(|a, b| a.0.cmp(&b.0))
                .map(|(_, name)| name);
            for (source_node, target_node) in sorted_nodes.tuple_windows() {
                update.add_event(UpdateEvent::AddEdge {
                    source_node,
                    target_node,
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: ordering_name.to_string(),
                })?;
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
        ordering: &mut BTreeMap<String, BTreeSet<(NodeSpec, String)>>,
    ) -> Result<usize> {
        let mut end_ids = BTreeSet::default();
        end_ids.insert(start_id); // to guarantee an option of a maximal value
        let lines = data.into_inner();
        let mut block_annos = Vec::with_capacity(lines.len());
        for line in lines {
            if line.as_rule() == Rule::line {
                let (end_id, block_anno) =
                    self.map_annotation_line(update, doc_node_name, line, start_id, ordering)?;
                end_ids.insert(end_id);
                if let Some(anno) = block_anno {
                    block_annos.push(anno);
                }
            }
        }
        if let Some(max_end) = end_ids.into_iter().max() {
            for (i, anno) in block_annos.into_iter().enumerate() {
                let tokens = (start_id..max_end).map(NodeSpec::Terminal).collect_vec();
                self.annotate(
                    update,
                    doc_node_name,
                    (start_id, i as u8, "span".to_string()),
                    tokens,
                    (&anno.key.ns, &anno.key.name, anno.val.trim()),
                    ordering,
                )?;
            }
            Ok(max_end)
        } else {
            Err(AnnattoError::Import {
                reason: "Could not determine end of span.".to_string(),
                importer: "toolbox".to_string(),
                path: Path::new(doc_node_name).to_path_buf(),
            })
        }
    }

    fn map_annotation_line(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        data: Pair<Rule>,
        start_id: usize,
        ordering: &mut BTreeMap<String, BTreeSet<(NodeSpec, String)>>,
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
                        ordering,
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
        ordering: &mut BTreeMap<String, BTreeSet<(NodeSpec, String)>>,
    ) -> Result<(usize, Option<String>)> {
        let inner = data.into_inner();
        // annotate nodes
        let mut timeline_id = id;
        let mut join_list = Vec::new();
        let build_joint = self.span.contains(anno_name);
        let use_tokens = self.target == anno_name;
        for entry_or_space in inner {
            match entry_or_space.as_rule() {
                Rule::entry => {
                    if build_joint {
                        join_list.push(entry_or_space.as_str());
                    } else {
                        let mut inner = entry_or_space.clone().into_inner();
                        let entry_node = if let Some(nxt) = inner.next() {
                            nxt
                        } else {
                            continue;
                        };
                        match entry_node.as_rule() {
                            Rule::complex => {
                                let sub_entries = entry_node.into_inner();
                                for (sub_index, sub_entry) in sub_entries.enumerate() {
                                    match sub_entry.as_rule() {
                                        Rule::default | Rule::pro_clitic | Rule::en_clitic => {
                                            self.annotate(
                                                update,
                                                doc_node_name,
                                                (
                                                    timeline_id,
                                                    sub_index as u8,
                                                    anno_name.to_string(),
                                                ),
                                                vec![if use_tokens {
                                                    NodeSpec::Terminal(timeline_id)
                                                } else {
                                                    NodeSpec::NonTerminal((
                                                        timeline_id,
                                                        0,
                                                        self.target.to_string(),
                                                    ))
                                                }],
                                                ("", anno_name, sub_entry.as_str()),
                                                ordering,
                                            )?;
                                        }
                                        _ => continue,
                                    };
                                }
                                timeline_id += 1;
                            }
                            _ => {
                                // with_dashes OR default
                                let (node_index, target_index) = (
                                    (timeline_id, 0_u8, anno_name.to_string()),
                                    if use_tokens {
                                        NodeSpec::Terminal(timeline_id)
                                    } else {
                                        NodeSpec::NonTerminal((
                                            timeline_id,
                                            0_u8,
                                            self.target.to_string(),
                                        ))
                                    },
                                );
                                self.annotate(
                                    update,
                                    doc_node_name,
                                    node_index,
                                    vec![target_index],
                                    ("", anno_name, entry_or_space.as_str()),
                                    ordering,
                                )?;
                                timeline_id += 1;
                            }
                        };
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

    fn annotate(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        node_index: AnnotationNode,
        targets: Vec<NodeSpec>,
        anno: (&str, &str, &str),
        ordering: &mut BTreeMap<String, BTreeSet<(NodeSpec, String)>>,
    ) -> Result<()> {
        let node_name = format!(
            "{doc_node_name}#{}{}.{}",
            node_index.2, node_index.0, node_index.1
        );
        update.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: anno.0.to_string(),
            anno_name: anno.1.to_string(),
            anno_value: anno.2.to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        let pointing_component =
            AnnotationComponent::new(AnnotationComponentType::Pointing, "".into(), anno.1.into());
        let coverage_component = AnnotationComponent::new(
            AnnotationComponentType::Coverage,
            ANNIS_NS.into(),
            "virtual".into(),
        );
        for target in targets {
            let (target_name, component) = match target {
                NodeSpec::NonTerminal((id, sub_id, prefix)) => (
                    format!("{doc_node_name}#{prefix}{id}.{sub_id}"),
                    &pointing_component,
                ),
                NodeSpec::Terminal(tok_i) => {
                    let target_name = format!("{doc_node_name}#{tok_i}");
                    match ordering.entry("".to_string()) {
                        Entry::Vacant(e) => {
                            update.add_event(UpdateEvent::AddNode {
                                node_name: target_name.to_string(),
                                node_type: "node".to_string(),
                            })?;
                            let mut v = BTreeSet::default();
                            v.insert((target, target_name.to_string()));
                            e.insert(v);
                        }
                        Entry::Occupied(mut e) => {
                            let k = (target, target_name.to_string());
                            let v = e.get_mut();
                            if !v.contains(&k) {
                                update.add_event(UpdateEvent::AddNode {
                                    node_name: target_name.to_string(),
                                    node_type: "node".to_string(),
                                })?;
                                v.insert(k);
                            }
                        }
                    }
                    (target_name, &coverage_component)
                }
            };
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: target_name,
                layer: component.layer.to_string(),
                component_type: component.get_type().to_string(),
                component_name: component.name.to_string(),
            })?;
        }
        update.add_event(UpdateEvent::AddEdge {
            source_node: node_name.to_string(),
            target_node: doc_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let ordered_node_spec =
            NodeSpec::NonTerminal((node_index.0, node_index.1, node_index.2.to_string()));
        match ordering.entry(anno.1.to_string()) {
            Entry::Vacant(e) => {
                let mut v = BTreeSet::default();
                v.insert((ordered_node_spec, node_name));
                e.insert(v);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().insert((ordered_node_spec, node_name));
            }
        };
        Ok(())
    }
}

type AnnotationNode = (usize, u8, String);
type Tok = usize;

#[derive(PartialOrd, PartialEq, Eq, Ord)]
enum NodeSpec {
    NonTerminal(AnnotationNode),
    Terminal(Tok),
}

/// This implements the Pest parser for the given grammar.
#[derive(Parser)]
#[grammar = "importer/toolbox/toolbox.pest"]
struct ToolboxParser;

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use insta::assert_snapshot;

    use crate::importer::toolbox::ImportToolBox;

    #[test]
    fn serialize() {
        let module = ImportToolBox::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = ImportToolBox {
            span: vec!["sentence".to_string(), "clause".to_string()]
                .into_iter()
                .collect(),
            target: "tx".to_string(),
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn core_functionality() {
        let ts = fs::read_to_string("tests/data/import/toolbox/build.toml");
        assert!(ts.is_ok(), "Could not read workflow: {:?}", ts.err());
        let toml_str = ts.unwrap();
        let imp: Result<ImportToolBox, _> = toml::from_str(toml_str.as_str());
        assert!(imp.is_ok(), "Error occurred: {:?}", imp.err());
        let importer = imp.unwrap();
        let graphml_is = crate::test_util::import_as_graphml_string(
            importer,
            Path::new("tests/data/import/toolbox/"),
            None,
        );
        assert!(
            graphml_is.is_ok(),
            "Failed to import test file: {:?}",
            graphml_is.err()
        );
        assert_snapshot!(graphml_is.unwrap());
    }
}
