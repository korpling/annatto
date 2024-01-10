use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use anyhow::anyhow;
use graphannis::{graph::GraphStorage, model::AnnotationComponentType, AnnotationGraph};
use graphannis_core::{
    annostorage::{NodeAnnotationStorage, ValueSearch},
    graph::ANNIS_NS,
    types::{AnnoKey, Component, NodeID},
    util::join_qname,
};
use linked_hash_map::LinkedHashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use umya_spreadsheet::{helper::coordinate::string_from_column_index, Worksheet};

use crate::{
    progress::ProgressReporter,
    util::token_helper::{TokenHelper, TOKEN_KEY},
    Module,
};

use super::Exporter;

pub const MODULE_NAME: &str = "export_xlsx";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct XlsxExporter {
    include_namespace: bool,
    annotation_order: Vec<String>,
}

impl Module for XlsxExporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

fn find_token_roots(
    g: &graphannis::AnnotationGraph,
    token_helper: &TokenHelper,
    ordering_gs: Option<&dyn GraphStorage>,
) -> anyhow::Result<HashSet<NodeID>> {
    let mut roots: HashSet<_> = HashSet::new();
    for n in g
        .get_node_annos()
        .exact_anno_search(Some(ANNIS_NS), "tok", ValueSearch::Any)
    {
        let n = n?;

        // Check that this is an actual token and there are no outgoing coverage edges
        if token_helper.is_token(n.node)?
            && (ordering_gs.is_none()
                || ordering_gs.is_some_and(|gs| gs.get_ingoing_edges(n.node).next().is_none()))
        {
            roots.insert(n.node);
        }
    }
    Ok(roots)
}

fn is_span_column(
    anno_key: &AnnoKey,
    node_annos: &dyn NodeAnnotationStorage,
    token_helper: &TokenHelper,
) -> anyhow::Result<bool> {
    // Check that none of the nodes having this key are token
    for m in node_annos.exact_anno_search(Some(&anno_key.ns), &anno_key.name, ValueSearch::Any) {
        let m = m?;
        if token_helper.is_token(m.node)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn overwritten_position_for_key(
    anno_key: &AnnoKey,
    position_overwrite: &HashMap<String, u32>,
) -> Option<u32> {
    // Try the fully qualified name first, then check if the unspecific name is configured
    position_overwrite
        .get(&join_qname(&anno_key.ns, &anno_key.name))
        .or_else(|| position_overwrite.get(anno_key.name.as_str()))
        .copied()
}

impl XlsxExporter {
    fn export_document(
        &self,
        doc_name: &str,
        doc_node_id: NodeID,
        g: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
    ) -> Result<(), anyhow::Error> {
        let mut book = umya_spreadsheet::new_file();
        let worksheet = book.get_active_sheet_mut();

        let token_helper = TokenHelper::new(g)?;
        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);

        let token_roots = find_token_roots(g, &token_helper, ordering_gs)?;

        let (token_to_row, has_only_empty_token) =
            self.create_token_colum(g, &token_roots, doc_node_id, worksheet)?;
        worksheet.get_cell_mut((1, 1)).set_value_string("tok");

        // Output all spans
        let name_to_column = self.get_span_columns(g, &token_helper, 2)?;
        self.create_span_columns(g, &name_to_column, token_to_row, &token_helper, worksheet)?;

        if has_only_empty_token {
            // Remove the empty token column
            worksheet.remove_column_by_index(&1, &1);
        }

        let output_path = output_path.join(format!("{}.xlsx", doc_name));
        umya_spreadsheet::writer::xlsx::write(&book, output_path)?;

        Ok(())
    }

    /// Get all annotation names for spans and assign them to a spreadsheet column.
    fn get_span_columns(
        &self,
        g: &AnnotationGraph,
        token_helper: &TokenHelper,
        column_offset: u32,
    ) -> anyhow::Result<LinkedHashMap<AnnoKey, u32>> {
        // create a hash map from the configuration value
        let position_overwrite: HashMap<String, u32> = self
            .annotation_order
            .iter()
            .enumerate()
            .map(|(idx, anno)| (anno.clone(), (idx as u32) + column_offset))
            .collect();

        let node_annos = g.get_node_annos();
        let mut all_anno_keys = node_annos.annotation_keys()?;

        // order the annotation keys by the configuration
        all_anno_keys.sort_by(|a, b| {
            let a_overwrite = overwritten_position_for_key(a, &position_overwrite);
            let b_overwrite = overwritten_position_for_key(b, &position_overwrite);

            if let (Some(a_overwrite), Some(b_overwrite)) = (a_overwrite, b_overwrite) {
                // Compare the configured values
                a_overwrite.cmp(&b_overwrite)
            } else if a_overwrite.is_some() {
                Ordering::Less
            } else if b_overwrite.is_some() {
                Ordering::Greater
            } else {
                // Use lexical comparision of the namespace/name components
                a.cmp(b)
            }
        });

        let mut result = LinkedHashMap::new();

        let mut column_index = column_offset;
        for anno_key in all_anno_keys {
            if anno_key.ns != ANNIS_NS && is_span_column(&anno_key, node_annos, token_helper)? {
                result.insert(anno_key, column_index);
                column_index += 1;
            }
        }

        Ok(result)
    }

    fn create_token_colum(
        &self,
        g: &AnnotationGraph,
        token_roots: &HashSet<NodeID>,
        doc_node_id: NodeID,
        worksheet: &mut Worksheet,
    ) -> anyhow::Result<(HashMap<NodeID, u32>, bool)> {
        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);
        if let Some(gs_part_of) = g.get_graphstorage_as_ref(&Component::new(
            AnnotationComponentType::PartOf,
            ANNIS_NS.into(),
            "".into(),
        )) {
            // Output all token in the first column
            let mut row_index = 1;
            let mut token_roots_for_document = Vec::default();
            for t in token_roots {
                if gs_part_of.is_connected(*t, doc_node_id, 1, std::ops::Bound::Unbounded)? {
                    token_roots_for_document.push(*t);
                }
            }

            let mut has_only_empty_token = true;

            let mut token_to_row = HashMap::new();

            // Start with the first token
            let mut token = token_roots_for_document.into_iter().next();

            while let Some(current_token) = token {
                if let Some(val) = g
                    .get_node_annos()
                    .get_value_for_item(&current_token, &TOKEN_KEY)?
                {
                    if !val.trim().is_empty() {
                        has_only_empty_token = false;
                    }
                    worksheet.get_cell_mut((1, row_index)).set_value_string(val);
                }

                // Reserve the first row for the header
                token_to_row.insert(current_token, row_index + 1);

                token = if let Some(ordering_gs) = ordering_gs {
                    if let Some(next_token) = ordering_gs.get_outgoing_edges(current_token).next() {
                        let next_token = next_token?;
                        Some(next_token)
                    } else {
                        None
                    }
                } else {
                    None
                };
                row_index += 1;
            }
            Ok((token_to_row, has_only_empty_token))
        } else {
            Err(anyhow!("Missing PartOf component"))
        }
    }

    fn create_span_columns(
        &self,
        g: &AnnotationGraph,
        name_to_column: &LinkedHashMap<AnnoKey, u32>,
        token_to_row: HashMap<NodeID, u32>,
        token_helper: &TokenHelper,
        worksheet: &mut Worksheet,
    ) -> anyhow::Result<()> {
        for span_anno_key in name_to_column.keys() {
            if let Some(column_index) = name_to_column.get(span_anno_key) {
                if self.include_namespace {
                    worksheet
                        .get_cell_mut((*column_index, 1))
                        .set_value_string(join_qname(&span_anno_key.ns, &span_anno_key.name));
                } else {
                    worksheet
                        .get_cell_mut((*column_index, 1))
                        .set_value_string(span_anno_key.name.clone());
                }
                for span in g.get_node_annos().exact_anno_search(
                    Some(&span_anno_key.ns),
                    &span_anno_key.name,
                    ValueSearch::Any,
                ) {
                    let span = span?;
                    let span_val = g
                        .get_node_annos()
                        .get_value_for_item(&span.node, &span.anno_key)?
                        .unwrap_or_default();

                    let mut spanned_rows = BTreeSet::new();
                    // Find all token covered by the span
                    for gs in token_helper.get_gs_coverage().iter() {
                        for t in gs.get_outgoing_edges(span.node) {
                            let t = t?;
                            if let Some(row) = token_to_row.get(&t) {
                                spanned_rows.insert(row);
                            }
                        }
                    }
                    let first_row = spanned_rows.first();
                    let last_row = spanned_rows.last();

                    if let (Some(first), Some(last)) = (first_row, last_row) {
                        let first_cell =
                            format!("{}{}", string_from_column_index(column_index), *first);
                        worksheet
                            .get_cell_mut(first_cell.clone())
                            .set_value_string(span_val);
                        let last_cell =
                            format!("{}{}", string_from_column_index(column_index), last);
                        worksheet.add_merge_cells(format!("{}:{}", first_cell, last_cell));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Exporter for XlsxExporter {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Find all documents in the graph
        let doc_annos = graph.get_node_annos().exact_anno_search(
            Some(ANNIS_NS),
            "doc",
            graphannis_core::annostorage::ValueSearch::Any,
        );

        let mut document_names = Vec::new();
        for m in doc_annos {
            let m = m?;
            if let Some(val) = graph
                .get_node_annos()
                .get_value_for_item(&m.node, &m.anno_key)?
            {
                document_names.push((val, m.node));
            }
        }

        let reporter = ProgressReporter::new(tx, step_id, document_names.len())?;

        std::fs::create_dir_all(output_path)?;

        let results: anyhow::Result<Vec<_>> = document_names
            .par_iter()
            .map(|(doc_name, doc_node_id)| {
                self.export_document(doc_name, *doc_node_id, graph, output_path)?;
                reporter.worked(1)?;
                Ok(())
            })
            .collect();
        results?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        "xlsx"
    }
}
