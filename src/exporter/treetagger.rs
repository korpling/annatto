use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
    usize,
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
}

fn default_doc_anno() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

impl Default for ExportTreeTagger {
    fn default() -> Self {
        Self {
            segmentation: None,
            doc_anno: default_doc_anno(),
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
    ) -> Result<(), anyhow::Error> {
        let node_annos = graph.get_node_annos();
        let doc_node_name = node_annos
            .get_value_for_item(&doc_node, &self.doc_anno)?
            .ok_or(anyhow!("Could not determine document node name."))?;
        let file_path =
            Path::new(corpus_path).join(format!("{doc_node_name}.{}", self.file_extension()));
        let mut writer = BufWriter::new(File::create(file_path)?);
        let it = dfs::CycleSafeDFS::new(gs_ordering.as_edgecontainer(), start_node, 0, usize::MAX);
        for token in it {
            let token = token?.node;
            let token_val = node_annos
                .get_value_for_item(&token, &TOKEN_KEY)?
                .unwrap_or_default();
            writer.write(token_val.as_bytes())?;
            writer.write("\n".as_bytes())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;
    use crate::{
        StepID,
        importer::{Importer as _, graphml::GraphMLImporter},
        test_util::export_to_string,
    };
    use std::path::Path;

    #[test]
    fn core() {
        let graphml = GraphMLImporter {};
        let import = graphml.import_corpus(
            Path::new("tests/data/import/graphml/single_sentence.graphml"),
            StepID {
                module_name: "test_import_graphml".to_string(),
                path: None,
            },
            None,
        );
        assert!(import.is_ok());
        let mut update_import = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let export = export_to_string(&graph, ExportTreeTagger::default());
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }
}
