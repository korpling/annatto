use std::collections::HashSet;

use graphannis::model::AnnotationComponentType;
use graphannis_core::{
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::{AnnoKey, Annotation, Component},
};
use serde::Deserialize;

use crate::{progress::ProgressReporter, Module};

use super::Manipulator;

pub const MODULE_NAME: &str = "annis_compat";

/// Checks that the annotation graph complies with assumptions made be AQL/the
/// ANNIS frontend and updates it when possible.
#[derive(Deserialize)]
pub struct AnnisCompatibility {}

impl Module for AnnisCompatibility {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Manipulator for AnnisCompatibility {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new(tx, self.step_id(None), 1)?;

        self.annis_doc_metadata(graph, &progress)?;
        progress.worked(1)?;
        Ok(())
    }
}
impl AnnisCompatibility {
    /// Each document should have an "annis::doc" annotation with its document name as value.
    fn annis_doc_metadata(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        progress: &ProgressReporter,
    ) -> crate::Result<()> {
        if let Some(part_of_gs) = graph.get_graphstorage(&Component::new(
            AnnotationComponentType::PartOf,
            ANNIS_NS.into(),
            "".into(),
        )) {
            let datasource_key = AnnoKey {
                ns: ANNIS_NS.into(),
                name: "datasource".into(),
            };
            let anno_doc_key = AnnoKey {
                ns: ANNIS_NS.into(),
                name: "doc".into(),
            };
            let mut document_nodes = HashSet::new();
            let node_annos = graph.get_node_annos_mut();
            for n in part_of_gs.source_nodes() {
                let n = n?;
                for n in part_of_gs.get_outgoing_edges(n) {
                    let n = n?;
                    // Leaf nodes are the ones with no outgoging PartOf edge
                    if part_of_gs.has_outgoing_edges(n)? == false {
                        progress.info(&format!("Found possible document node {n}"))?;
                        // Leaf nodes can be either document or datasources (like
                        // texts). Find the ones that are documents and get the
                        // parent nodes for the datasources.
                        if node_annos
                            .get_value_for_item(&n, &datasource_key)?
                            .is_some()
                        {
                            // Parent node of this datasource is a document
                            if let Some(parent) = part_of_gs.get_ingoing_edges(n).next() {
                                let parent = parent?;
                                document_nodes.insert(parent);
                            }
                        } else {
                            // The node itself is  document
                            document_nodes.insert(n);
                        }
                    }
                }
            }
            progress.info(&format!("Found {} document nodes-", document_nodes.len()))?;
            for doc_node_id in document_nodes {
                // Only add annotation if it does not exist yet
                if node_annos
                    .get_value_for_item(&doc_node_id, &anno_doc_key)?
                    .is_none()
                {
                    progress.info(&format!(
                        "Document {} has no annis::doc anno",
                        node_annos
                            .get_value_for_item(&doc_node_id, &NODE_NAME_KEY)?
                            .unwrap_or_default(),
                    ))?;
                    // The node name can be a simple string or an URI, parse as
                    // URI and use the last part of the path as document name.

                    if let Some(node_name) =
                        node_annos.get_value_for_item(&doc_node_id, &NODE_NAME_KEY)?
                    {
                        let parsed = url::Url::parse(&node_name)?;

                        let doc_name = parsed
                            .path_segments()
                            .and_then(|p| p.last())
                            .unwrap_or_else(|| &node_name);
                        dbg!(doc_name);
                        node_annos.insert(
                            doc_node_id,
                            Annotation {
                                key: anno_doc_key.clone(),
                                val: doc_name.into(),
                            },
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}
