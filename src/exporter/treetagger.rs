use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, GraphStorage, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    dfs::{self, CycleSafeDFS},
    graph::{ANNIS_NS, DEFAULT_NS, NODE_NAME_KEY},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

use crate::{
    progress::ProgressReporter,
    util::token_helper::{TOKEN_KEY, TokenHelper},
};

/// Exporter for the file format used by the TreeTagger.
#[derive(
    Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize, Clone, PartialEq,
)]
#[serde(deny_unknown_fields)]
pub struct ExportTreeTagger {
    /// Provide the token annotation names that should be exported as columns.
    /// If you do not provide a namespace, "default_ns" will be used
    /// automatically.
    #[serde(
        default = "default_column_names",
        with = "crate::estarde::anno_key::in_sequence"
    )]
    column_names: Vec<AnnoKey>,
    /// If given, use this segmentation instead of the token as token column.
    #[serde(default)]
    segmentation: Option<String>,
    /// The provided annotation key defines which nodes within the part-of component define a document. All nodes holding said annotation
    /// will be exported to a file with the name according to the annotation value. Therefore annotation values must not contain path
    /// delimiters.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// doc_anno = "my_namespace::document"
    /// ```
    ///
    /// The default is `annis::doc`.
    #[serde(default = "default_doc_anno", with = "crate::estarde::anno_key")]
    doc_anno: AnnoKey,
    /// Don't output meta data header when set to `true`
    skip_meta: bool,
    /// Don't output SGML tags for span annotations when set to `true`
    skip_spans: bool,
}

fn default_doc_anno() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_column_names() -> Vec<AnnoKey> {
    vec![
        AnnoKey {
            name: "pos".into(),
            ns: DEFAULT_NS.into(),
        },
        AnnoKey {
            name: "lemma".into(),
            ns: DEFAULT_NS.into(),
        },
    ]
}

impl Default for ExportTreeTagger {
    fn default() -> Self {
        Self {
            column_names: default_column_names(),
            segmentation: None,
            doc_anno: default_doc_anno(),
            skip_meta: false,
            skip_spans: false,
        }
    }
}

const FILE_EXTENSION: &str = "tt";

impl Exporter for ExportTreeTagger {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;

        let base_ordering = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );

        let selected_ordering = if let Some(seg) = &self.segmentation {
            let matching_components =
                graph.get_all_components(Some(AnnotationComponentType::Ordering), Some(seg));
            if matching_components.len() == 1 {
                matching_components[0].clone()
            } else if let Some(c) = matching_components.iter().find(|c| c.layer.as_str() == seg) {
                c.clone()
            } else if let Some(c) = matching_components
                .iter()
                .find(|c| c.layer.as_str() == ANNIS_NS)
            {
                c.clone()
            } else if let Some(c) = matching_components
                .iter()
                .find(|c| c.layer.as_str() == DEFAULT_NS)
            {
                c.clone()
            } else {
                base_ordering
            }
        } else {
            base_ordering
        };

        let gs_ordering = graph
            .get_graphstorage(&selected_ordering)
            .ok_or(anyhow!("Storage of ordering component unavailable"))?;
        let part_of_storage = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            ))
            .ok_or(anyhow!("Part-of storage unavailable."))?;

        let mut doc_node_to_start = BTreeMap::new();
        for node in gs_ordering.root_nodes() {
            let node = node?;
            let dfs = CycleSafeDFS::new(
                part_of_storage.as_edgecontainer(),
                node,
                0,
                NodeID::MAX as usize,
            );
            for nxt in dfs {
                let n = nxt?.node;
                if graph
                    .get_node_annos()
                    .has_value_for_item(&n, &self.doc_anno)
                    .unwrap_or_default()
                {
                    if let Entry::Vacant(e) = doc_node_to_start.entry(n) {
                        e.insert(node);
                        break;
                    } else {
                        let doc_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(&n, &NODE_NAME_KEY)?
                            .unwrap_or_default();
                        return Err(anyhow!(
                            "Document {doc_node_name} has more than one start node for base ordering."
                        )
                        .into());
                    }
                }
            }
        }
        let progress = ProgressReporter::new(tx, step_id, doc_node_to_start.len())?;
        progress.info(&format!("Exporting {} documents", doc_node_to_start.len()))?;
        doc_node_to_start
            .into_iter()
            .try_for_each(move |(doc, start)| -> anyhow::Result<()> {
                self.export_document(graph, output_path, doc, start, gs_ordering.clone())?;
                progress.worked(1)?;
                Ok(())
            })?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

impl ExportTreeTagger {
    fn export_document(
        &self,
        graph: &AnnotationGraph,
        corpus_path: &Path,
        doc_node: NodeID,
        start_node: NodeID,
        gs_ordering: Arc<dyn GraphStorage>,
    ) -> anyhow::Result<()> {
        let token_helper = TokenHelper::new(graph)?;

        let node_annos = graph.get_node_annos();
        let doc_node_name = node_annos
            .get_value_for_item(&doc_node, &self.doc_anno)?
            .ok_or(anyhow!("Could not determine document node name."))?;
        let file_path =
            Path::new(corpus_path).join(format!("{doc_node_name}.{}", self.file_extension()));
        let mut w = BufWriter::new(File::create(file_path)?);

        let footer = if self.skip_meta {
            None
        } else {
            Some(self.write_metadata_header(graph, doc_node, &mut w)?)
        };

        let it = dfs::CycleSafeDFS::new(gs_ordering.as_edgecontainer(), start_node, 0, usize::MAX);
        for token in it {
            let token = token?.node;

            if !self.skip_spans {
                self.write_starting_spans(graph, token, &token_helper, &mut w)?;
            }

            let token_val = node_annos
                .get_value_for_item(&token, &TOKEN_KEY)?
                .unwrap_or_default();
            write!(w, "{token_val}")?;
            for column in &self.column_names {
                let anno_value = node_annos
                    .get_value_for_item(&token, column)?
                    .unwrap_or_default();
                write!(w, "\t{anno_value}")?;
            }
            writeln!(w)?;

            if !self.skip_spans {
                self.write_ending_spans(graph, token, &token_helper, &mut w)?;
            }
        }

        if let Some(footer) = footer {
            writeln!(w, "{footer}")?;
        }
        Ok(())
    }

    /// Writes the metadata of this document as line with a span, returns the
    /// end-tag that needs to be added at the end.
    fn write_metadata_header<W: Write>(
        &self,
        graph: &AnnotationGraph,
        doc_node: NodeID,
        mut w: W,
    ) -> anyhow::Result<String> {
        write!(w, "<doc")?;

        for anno in graph.get_node_annos().get_annotations_for_item(&doc_node)? {
            if anno.key.ns != ANNIS_NS {
                let name = quick_xml::escape::escape(&anno.key.name);
                let value = quick_xml::escape::escape(&anno.val);
                write!(w, " {name}=\"{value}\"")?;
            }
        }
        writeln!(w, ">")?;

        Ok("</doc>".to_string())
    }

    /// Finds all spans that start at the given token and write their annotation values out.
    fn write_starting_spans<W: Write>(
        &self,
        graph: &AnnotationGraph,
        token: NodeID,
        token_helper: &TokenHelper,
        mut w: W,
    ) -> anyhow::Result<()> {
        if let Some(left_token) = token_helper.left_token_for(token)? {
            for starting_span in token_helper
                .get_gs_left_token()
                .get_ingoing_edges(left_token)
            {
                let starting_span = starting_span?;
                // Ignore segmentation spans
                if !graph
                    .get_node_annos()
                    .has_value_for_item(&starting_span, &TOKEN_KEY)?
                {
                    let tag = self.tag_name_for_span(graph, starting_span)?;
                    write!(w, "<{tag}")?;
                    for anno in graph
                        .get_node_annos()
                        .get_annotations_for_item(&starting_span)?
                    {
                        if anno.key.ns != ANNIS_NS {
                            let name = quick_xml::escape::escape(&anno.key.name);
                            let value = quick_xml::escape::escape(&anno.val);
                            write!(w, " {name}=\"{value}\"")?;
                        }
                    }
                    writeln!(w, ">")?;
                }
            }
        }

        Ok(())
    }

    /// Finds all spans that end at the given token and write their annotation values out.
    fn write_ending_spans<W: Write>(
        &self,
        graph: &AnnotationGraph,
        token: NodeID,
        token_helper: &TokenHelper,
        mut w: W,
    ) -> anyhow::Result<()> {
        if let Some(right_token) = token_helper.right_token_for(token)? {
            for ending_span in token_helper
                .get_gs_right_token()
                .get_ingoing_edges(right_token)
            {
                let ending_span = ending_span?;
                // Ignore segmentation spans
                if !graph
                    .get_node_annos()
                    .has_value_for_item(&ending_span, &TOKEN_KEY)?
                {
                    let tag = self.tag_name_for_span(graph, ending_span)?;
                    writeln!(w, "</{tag}>")?;
                }
            }
        }

        Ok(())
    }

    fn tag_name_for_span(&self, graph: &AnnotationGraph, span: NodeID) -> anyhow::Result<String> {
        let keys: Vec<_> = graph
            .get_node_annos()
            .get_all_keys_for_item(&span, None, None)?
            .into_iter()
            .filter(|key| key.ns != ANNIS_NS)
            .sorted()
            .collect();
        let first_name = keys
            .first()
            .map(|key| key.name.to_string())
            .unwrap_or_else(|| "span".to_string());
        Ok(first_name)
    }
}

#[cfg(test)]
mod tests;
