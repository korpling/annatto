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
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

use crate::{progress::ProgressReporter, util::token_helper::TOKEN_KEY};

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
            write!(w, "\n")?;
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
}

#[cfg(test)]
mod tests {
    use graphannis::update::GraphUpdate;
    use insta::assert_snapshot;

    use super::*;
    use crate::{
        test_util::export_to_string,
        util::example_generator::{self, add_node_label, make_span},
    };

    fn create_test_corpus_base_token() -> AnnotationGraph {
        let mut u = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut u);
        example_generator::create_tokens(&mut u, Some("root/doc1"));
        make_span(
            &mut u,
            &format!("root/doc1#span1"),
            &[&format!("root/doc1#tok0"), &format!("root/doc1#tok1")],
            true,
        );
        // Add POS annotations
        add_node_label(&mut u, "root/doc1#tok0", "default_ns", "pos", "VBZ");
        add_node_label(&mut u, "root/doc1#tok1", "default_ns", "pos", "DT");
        add_node_label(&mut u, "root/doc1#tok2", "default_ns", "pos", "NN");
        add_node_label(&mut u, "root/doc1#tok3", "default_ns", "pos", "RBR");
        add_node_label(&mut u, "root/doc1#tok4", "default_ns", "pos", "JJ");
        add_node_label(&mut u, "root/doc1#tok5", "default_ns", "pos", "IN");
        add_node_label(&mut u, "root/doc1#tok6", "default_ns", "pos", "PP");
        add_node_label(&mut u, "root/doc1#tok7", "default_ns", "pos", "VBZ");
        add_node_label(&mut u, "root/doc1#tok8", "default_ns", "pos", "TO");
        add_node_label(&mut u, "root/doc1#tok9", "default_ns", "pos", "VB");
        add_node_label(&mut u, "root/doc1#tok10", "default_ns", "pos", "SENT");

        // Add lemma annotations
        add_node_label(&mut u, "root/doc1#tok0", "default_ns", "lemma", "be");
        add_node_label(&mut u, "root/doc1#tok1", "default_ns", "lemma", "this");
        add_node_label(&mut u, "root/doc1#tok2", "default_ns", "lemma", "example");
        add_node_label(&mut u, "root/doc1#tok3", "default_ns", "lemma", "more");
        add_node_label(
            &mut u,
            "root/doc1#tok4",
            "default_ns",
            "lemma",
            "complicated",
        );
        add_node_label(&mut u, "root/doc1#tok5", "default_ns", "lemma", "than");
        add_node_label(&mut u, "root/doc1#tok6", "default_ns", "lemma", "it");
        add_node_label(&mut u, "root/doc1#tok7", "default_ns", "lemma", "appear");
        add_node_label(&mut u, "root/doc1#tok8", "default_ns", "lemma", "to");
        add_node_label(&mut u, "root/doc1#tok9", "default_ns", "lemma", "be");
        add_node_label(&mut u, "root/doc1#tok10", "default_ns", "lemma", "?");

        // Add some additional metadata to the document
        add_node_label(&mut u, "root/doc1", "ignored", "author", "<unknown>");
        add_node_label(&mut u, "root/doc1", "default_ns", "year", "1984");

        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut u, |_| {}).is_ok());
        graph
    }

    #[test]
    fn core() {
        let graph = create_test_corpus_base_token();

        let export_config = ExportTreeTagger::default();

        let export = export_to_string(&graph, export_config);
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn core_no_metadata() {
        let graph = create_test_corpus_base_token();

        let mut export_config = ExportTreeTagger::default();
        export_config.skip_meta = true;

        let export = export_to_string(&graph, export_config);
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }
}
