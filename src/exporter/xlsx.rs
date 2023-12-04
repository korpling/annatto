use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use graphannis::{graph::GraphStorage, model::AnnotationComponentType};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::ANNIS_NS,
    types::{AnnoKey, Component, NodeID},
};
use serde::Deserialize;

use crate::{progress::ProgressReporter, Module};

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
    cov_edges: &Vec<Arc<dyn GraphStorage>>,
    ordering_gs: Option<&dyn GraphStorage>,
) -> anyhow::Result<HashSet<NodeID>> {
    let mut roots: HashSet<_> = HashSet::new();
    for n in g
        .get_node_annos()
        .exact_anno_search(Some(ANNIS_NS), "tok", ValueSearch::Any)
    {
        let n = n?;

        // Check that this is an actual token and there are no outgoing coverage edges
        let mut actual_token = true;
        for c in cov_edges.iter() {
            if c.has_outgoing_edges(n.node)? {
                actual_token = false;
                break;
            }
        }
        if actual_token
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

        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);

        let cov_edges: Vec<Arc<dyn GraphStorage>> = g
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .into_iter()
            .filter_map(|c| g.get_graphstorage(&c))
            .filter(|gs| {
                if let Some(stats) = gs.get_statistics() {
                    stats.nodes > 0
                } else {
                    true
                }
            })
            .collect();

        // Output all token in the first column
        let roots = find_token_roots(g, &cov_edges, ordering_gs)?;
        let mut row_index = 1;
        let mut token = roots.iter().next().copied();

        let mut token_to_row = HashMap::new();

        while let Some(current_token) = token {
            let token_value_key = AnnoKey {
                ns: ANNIS_NS.into(),
                name: "tok".into(),
            };

            if let Some(val) = g
                .get_node_annos()
                .get_value_for_item(&current_token, &token_value_key)?
            {
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
                dbg!(&span);
                let mut spanned_rows = BTreeSet::new();
                // Find all token covered by the span
                for gs in cov_edges.iter() {
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
                    worksheet.add_merge_cells(format!(
                        "{}:{}{}",
                        first_cell,
                        name_to_column.get(span_name).unwrap_or(&"A"),
                        last
                    ));
                }
            }
        }

        let output_path = output_path.join(format!("{}.xlsx", doc_name));
        umya_spreadsheet::writer::xlsx::write(&book, output_path)?;

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
                document_names.push(val);
            }
        }

        let reporter = ProgressReporter::new(tx, step_id, document_names.len())?;

        std::fs::create_dir_all(output_path)?;

        for doc in document_names {
            self.export_document(&doc, graph, output_path)?;
            reporter.worked(1)?;
        }
        Ok(())
    }
}
