use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use linked_hash_set::LinkedHashSet;
use pest::{Parser, iterators::Pairs};
use pest_derive::Parser;
use serde::{Deserialize, Serialize};

use crate::{error::AnnattoError, importer::Importer};

/// Import annotations provided in the fieldlinguist's toolbox text format.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportFLToolbox {
    /// The annotation names are considered global spans, i. e., they will cover all following
    /// annotations' tokens until a follow-up value is defined. Also, they are considered single-valued
    /// per line.
    #[serde(default)]
    globals: BTreeSet<String>,
    /// The annotation names named here are considered single-valued per line. Space values
    /// are not considered delimiters, but part of the annotation value. Such annotations
    /// rely on the existence of the target nodes, i. e. annotation lines without any other
    /// non-spanning annotation in the block will be dropped.
    #[serde(default)]
    span: BTreeSet<String>,
    /// Lists the annotation markers to be ignored.
    #[serde(default)]
    ignore: BTreeSet<String>,
    /// Null values are represented as `-` in toolbox. If you want those to remain explicit
    /// annotations, set `explicit_null = true`.
    #[serde(default)]
    explicit_null: bool,
}

impl Importer for ImportFLToolbox {
    fn import_corpus(
        &self,
        input_path: &Path,
        _step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let named_paths = config.derive_corpus_graph(input_path, &mut update)?;
        named_paths
            .into_iter()
            .try_for_each(|(p, d)| self.import_document(&p, &d, &mut update))?;
        Ok(update)
    }

    fn default_file_extensions(&self) -> &[&str] {
        &ImportFLToolbox::DEFAULT_FILE_EXTENSIONS
    }
}

impl ImportFLToolbox {
    pub(crate) const DEFAULT_FILE_EXTENSIONS: [&str; 1] = ["txt"];

    fn import_document(
        &self,
        path: &Path,
        doc_node_name: &str,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        let content = fs::read_to_string(path)?;
        let mut parsed_data = FLTBParser::parse(Rule::data, &content).map_err(|e| {
            AnnattoError::PestParsingError {
                file_path: path.to_path_buf(),
                error: e.to_string(),
            }
        })?;
        self.map_data(
            parsed_data
                .next()
                .ok_or::<AnnattoError>(anyhow!("Parsing error").into())?
                .into_inner(),
            doc_node_name,
            update,
        )?;
        Ok(())
    }

    fn map_data(
        &self,
        data: Pairs<Rule>,
        doc_node_name: &str,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        let mut order_from = None;
        let mut global_values = BTreeMap::default();
        for block in data {
            order_from = self.map_block(
                block.into_inner(),
                doc_node_name,
                order_from,
                &mut global_values,
                update,
            )?;
        }
        Ok(())
    }

    fn map_block(
        &self,
        block: Pairs<Rule>,
        doc_node_name: &str,
        mut continue_ordering_at: Option<String>,
        global_values: &mut BTreeMap<String, Vec<u8>>,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<Option<String>> {
        let mut byte_grid = Vec::with_capacity(6); // to find 4 tiers only is unlikely
        let mut all_slots = LinkedHashSet::<usize>::default(); // for sanity check
        let line_n = block.peek().map(|l| l.line_col().0).unwrap_or_default();
        let mut span_values = Vec::with_capacity(self.span.len());
        for line in block.into_iter() {
            if let Some(line_content) = self.read_line(line.into_inner())? {
                match line_content {
                    LineContent::GridMember { grid_line, .. } => {
                        all_slots.extend(&grid_line.slots);
                        byte_grid.push(grid_line);
                    }
                    LineContent::Span {
                        marker,
                        value,
                        comment,
                    } => {
                        if self.span.contains(&marker) {
                            span_values.push((marker, value));
                            if !comment.is_empty() {
                                span_values.push(("inline_comment".to_string(), comment));
                            }
                        } else if self.globals.contains(&marker) {
                            global_values.insert(marker, value);
                        }
                    }
                }
            }
        }
        if byte_grid.is_empty() {
            return Ok(continue_ordering_at);
        }
        let mut slot_map = BTreeMap::default();
        let all_slots = all_slots.into_iter().sorted_unstable().collect_vec(); // is this somehow avoidable?
        let block_span = if span_values.is_empty() {
            None
        } else {
            let span = format!("{doc_node_name}#s{line_n}");
            update.add_event(UpdateEvent::AddNode {
                node_name: span.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: span.to_string(),
                target_node: doc_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            for (k, v) in span_values {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: "".to_string(),
                    anno_name: k,
                    anno_value: str::from_utf8(&v)?.to_string(),
                })?;
            }
            for (k, v) in global_values {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: "".to_string(),
                    anno_name: k.to_string(),
                    anno_value: str::from_utf8(v)?.to_string(),
                })?;
            }
            Some(span)
        };
        for slot in &all_slots {
            let tok_name = format!("{doc_node_name}#t{line_n}_{slot}");
            update.add_event(UpdateEvent::AddNode {
                node_name: tok_name.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: tok_name.to_string(),
                target_node: doc_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if let Some(predecessor) = continue_ordering_at {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: predecessor,
                    target_node: tok_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if let Some(span_name) = &block_span {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: span_name.to_string(),
                    target_node: tok_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            continue_ordering_at = Some(tok_name.to_string());
            slot_map.insert(*slot, tok_name);
        }
        for grid_line in byte_grid {
            let mut byte_index = 0;
            let mut latest_node_name = None;
            for slot in &all_slots {
                let node_name = if grid_line.slots.contains(slot) {
                    let name = format!("{doc_node_name}#{}_{line_n}_{slot}", grid_line.marker);
                    update.add_event(UpdateEvent::AddNode {
                        node_name: name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "layer".to_string(),
                        anno_value: "default_layer".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: name.to_string(),
                        target_node: doc_node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                    let anno_value_raw = grid_line.bytes.get(byte_index).ok_or(anyhow!(
                        "No value available for marker {} in slot {}.",
                        grid_line.marker,
                        slot
                    ))?;
                    let anno_value = str::from_utf8(anno_value_raw)?.trim();
                    byte_index += 1;
                    if anno_value != "-" || self.explicit_null {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: name.to_string(),
                            anno_ns: "".to_string(),
                            anno_name: grid_line.marker.to_string(),
                            anno_value: anno_value.to_string(),
                        })?;
                    }
                    name
                } else {
                    latest_node_name
                        .as_ref()
                        .map(ToString::to_string)
                        .ok_or::<AnnattoError>(
                            anyhow!(
                                "Invalid: line {} in block in line {} has no starting entry",
                                grid_line.marker,
                                line_n
                            )
                            .into(),
                        )?
                };
                latest_node_name = Some(node_name.to_string());
                if let Some(tok_name) = slot_map.get(slot) {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: node_name.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
            }
        }
        Ok(continue_ordering_at)
    }

    fn read_line(&self, mut line: Pairs<Rule>) -> crate::error::Result<Option<LineContent>> {
        let marker = line
            .next()
            .ok_or(anyhow!("Line has no marker: `{}`", line.as_str()))?
            .as_str()
            .to_string();
        if marker.starts_with("_") || self.ignore.contains(&marker) {
            return Ok(None);
        }
        // parse as grid line
        let line_data = if let Some(ldata) = line.next() {
            ldata.as_str()
        } else {
            return Ok(None);
        };
        let comment = if let Some(comment) = line.next() {
            comment.as_str().bytes().dropping(3).collect()
        } else {
            vec![]
        };
        let mut slots = LinkedHashSet::default();
        if self.span.contains(&marker) || self.globals.contains(&marker) {
            return Ok(Some(LineContent::Span {
                marker,
                value: line_data.bytes().collect(),
                comment,
            }));
        }
        let mut bytes = Vec::<Vec<u8>>::with_capacity(8);
        let mut reading = false;
        for (i, b) in line_data.bytes().enumerate() {
            if reading {
                if b == 32 {
                    reading = false;
                } else {
                    if let Some(latest_byte_collection) = bytes.last_mut() {
                        latest_byte_collection.push(b);
                    }
                }
            } else {
                if b != 32 {
                    reading = true;
                    slots.insert(i);
                    bytes.push(Vec::with_capacity(8)); // assume that 8 bytes suffice for the majority of words (trade-off between repeated over-allocation and repeated re-allocation)
                    if let Some(latest_byte_collection) = bytes.last_mut() {
                        latest_byte_collection.push(b);
                    }
                }
            }
        }

        Ok(Some(LineContent::GridMember {
            grid_line: ByteGridLine {
                marker,
                slots,
                bytes,
            },
        }))
    }
}

enum LineContent {
    GridMember {
        grid_line: ByteGridLine,
    },
    Span {
        marker: String,
        value: Vec<u8>,
        comment: Vec<u8>,
    },
}

struct ByteGridLine {
    marker: String,
    slots: LinkedHashSet<usize>,
    bytes: Vec<Vec<u8>>,
}

#[derive(Parser)]
#[grammar = "importer/toolbox/minimal_grammar.pest"]
struct FLTBParser;

#[cfg(test)]
mod tests;
