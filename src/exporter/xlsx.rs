use std::collections::{BTreeSet, HashMap, HashSet};

use graphannis::{graph::GraphStorage, model::AnnotationComponentType, AnnotationGraph};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::ANNIS_NS,
    types::{Component, NodeID},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use umya_spreadsheet::Worksheet;

use crate::{
    progress::ProgressReporter,
    util::token_helper::{TokenHelper, TOKEN_KEY},
    Module,
};

use super::Exporter;

pub const MODULE_NAME: &str = "export_xlsx";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct XlsxExporter {}

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

impl XlsxExporter {
    fn export_document(
        &self,
        doc_name: &str,
        g: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
    ) -> Result<(), anyhow::Error> {
        let mut book = umya_spreadsheet::new_file();
        let worksheet = book.get_active_sheet_mut();

        let token_helper = TokenHelper::new(g)?;

        let (token_to_row, has_only_empty_token) =
            self.create_token_colum(g, &token_helper, worksheet)?;

        // Output all spans
        // TODO: do not hard-code all span annotation names
        let mut name_to_column = HashMap::new();
        name_to_column.insert("dipl", "B");
        name_to_column.insert("clean", "C");
        name_to_column.insert("lb", "D");

        for span_name in ["dipl", "clean", "lb"] {
            for span in g
                .get_node_annos()
                .exact_anno_search(None, span_name, ValueSearch::Any)
            {
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
                    let first_cell = format!(
                        "{}{}",
                        name_to_column.get(span_name).unwrap_or(&"A"),
                        *first
                    );
                    worksheet
                        .get_cell_mut(first_cell.clone())
                        .set_value_string(span_val);
                    let last_cell =
                        format!("{}{}", name_to_column.get(span_name).unwrap_or(&"A"), last);
                    worksheet.add_merge_cells(format!("{}:{}", first_cell, last_cell));
                }
            }
        }

        if has_only_empty_token {
            // Remove the token column
            worksheet.remove_column_by_index(&1, &1);
        }

        let output_path = output_path.join(format!("{}.xlsx", doc_name));
        umya_spreadsheet::writer::xlsx::write(&book, output_path)?;

        Ok(())
    }

    fn create_token_colum(
        &self,
        g: &AnnotationGraph,
        token_helper: &TokenHelper,
        worksheet: &mut Worksheet,
    ) -> anyhow::Result<(HashMap<NodeID, u32>, bool)> {
        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);

        // Output all token in the first column
        let roots = find_token_roots(g, token_helper, ordering_gs)?;
        let mut row_index = 1;
        let mut token = roots.iter().next().copied();
        let mut has_only_empty_token = true;

        let mut token_to_row = HashMap::new();

        while let Some(current_token) = token {
            if let Some(val) = g
                .get_node_annos()
                .get_value_for_item(&current_token, &TOKEN_KEY)?
            {
                if !val.trim().is_empty() {
                    has_only_empty_token = false;
                }
                worksheet
                    .get_cell_mut((1, row_index))
                    .set_value(val.to_string());
            }

            token_to_row.insert(current_token, row_index);

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
                document_names.push(val);
            }
        }

        let reporter = ProgressReporter::new(tx, step_id, document_names.len())?;

        std::fs::create_dir_all(output_path)?;

        let results: anyhow::Result<Vec<_>> = document_names
            .par_iter()
            .map(|doc| {
                self.export_document(&doc, graph, output_path)?;
                reporter.worked(1)?;
                Ok(())
            })
            .collect();
        results?;
        Ok(())
    }
}
