use std::collections::BTreeSet;

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    aql,
    graph::NodeID,
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Manipulator;

/// This module acts as a positive filter, i. e., all nodes that do not match the query and are not real tokens
/// are deleted. In inverse mode, all matching nodes (except real tokens) get deleted. This only applies to nodes
/// that are of node type "node". Other node types will be ignored.
///
/// The following example configuration deletes all nodes that are annotated to be nouns and are not real tokens:
/// ```toml
/// [[graph_op]]
/// action = "filter"
///
/// [graph_op.config]
/// query = "pos=/NOUN/"
/// inverse = true
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct FilterNodes {
    /// The AQL query to use to identify all relevant nodes.
    ///
    /// Example:
    /// ```toml
    /// [graph_op.config]
    /// query = "pos=/NOUN/"
    /// ```
    query: String,
    /// If this is set to true, all matching nodes, that are not coverage terminals ("real tokens"), are deleted. If false (default),
    /// the matching nodes and all real tokens are preserved, all other nodes are deleted.
    ///
    /// Example:
    /// ```toml
    /// [graph_op.config]
    /// query = "pos=/NOUN/"
    /// inverse = true
    /// ```
    #[serde(default)]
    inverse: bool,
}

impl Manipulator for FilterNodes {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let query = aql::parse(&self.query, false)?;
        let mut matching_nodes = BTreeSet::default();
        let node_annos = graph.get_node_annos();
        // collect timeline nodes along component "Ordering/annis/" to also keep the timeline
        let terminals = {
            let mut v = BTreeSet::default();
            if let Some(storage) = graph.get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "".into(),
            )) {
                let roots = storage
                    .source_nodes()
                    .flatten()
                    .filter(|n| !storage.has_ingoing_edges(*n).unwrap_or_default());
                for root in roots {
                    storage
                        .find_connected(root, 0, std::ops::Bound::Excluded(usize::MAX))
                        .flatten()
                        .for_each(|n| {
                            v.insert(n);
                        });
                }
            }
            v
        };
        aql::execute_query_on_graph(graph, &query, true, None)?
            .flatten()
            .for_each(|group| {
                for member in group {
                    matching_nodes.insert(member.node);
                }
            });
        if self.inverse {
            // delete matching nodes (without terminals aka real tokens)
            for n in matching_nodes.difference(&terminals) {
                if let Some(node_name) = node_annos.get_value_for_item(n, &NODE_NAME_KEY)? {
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: node_name.to_string(),
                    })?;
                } else {
                    return Err(anyhow!("Node has no name. This is invalid.").into());
                }
            }
        } else {
            // delete non-matching nodes of type "node" (excluding real tokens)
            let max_id = node_annos.get_largest_item()?.unwrap_or(NodeID::MAX);
            for n in 0..max_id {
                if let Some(node_type) = node_annos.get_value_for_item(&n, &NODE_TYPE_KEY)? {
                    if !matching_nodes.contains(&n)
                        && !terminals.contains(&n)
                        && &*node_type == "node"
                    {
                        if let Some(node_name) =
                            node_annos.get_value_for_item(&n, &NODE_NAME_KEY)?
                        {
                            update.add_event(UpdateEvent::DeleteNode {
                                node_name: node_name.to_string(),
                            })?;
                        } else {
                            return Err(anyhow!("Node has no name. This is invalid.").into());
                        }
                    }
                }
            }
        }
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{exmaralda::ImportEXMARaLDA, Importer},
        manipulator::{filter::FilterNodes, Manipulator},
        test_util::export_to_string,
        StepID,
    };

    #[test]
    fn default() {
        let exmaralda = ImportEXMARaLDA {};
        let mprt = exmaralda.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/"),
            StepID {
                module_name: "test_import_exb".to_string(),
                path: None,
            },
            None,
        );
        assert!(mprt.is_ok());
        let mut update_import = mprt.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let manipulation = FilterNodes {
            query: "pos=/PRON/".to_string(),
            inverse: false,
        };
        assert!(manipulation
            .manipulate_corpus(
                &mut graph,
                Path::new("./"),
                StepID {
                    module_name: "test_filter".to_string(),
                    path: None
                },
                None
            )
            .is_ok());
        let export = export_to_string(&graph, GraphMLExporter::default());
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn inverse() {
        let exmaralda = ImportEXMARaLDA {};
        let mprt = exmaralda.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/"),
            StepID {
                module_name: "test_import_exb".to_string(),
                path: None,
            },
            None,
        );
        assert!(mprt.is_ok());
        let mut update_import = mprt.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let manipulation = FilterNodes {
            query: "pos=/PRON/".to_string(),
            inverse: true,
        };
        assert!(manipulation
            .manipulate_corpus(
                &mut graph,
                Path::new("./"),
                StepID {
                    module_name: "test_filter".to_string(),
                    path: None
                },
                None
            )
            .is_ok());
        let export = export_to_string(&graph, GraphMLExporter::default());
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn deserialize() {
        let toml_str =
            fs::read_to_string(Path::new("./tests/data/graph_op/filter/deserialize.toml"))
                .unwrap_or_default();
        let filter_nodes: Result<FilterNodes, _> = toml::from_str(toml_str.as_str());
        assert!(filter_nodes.is_ok(), "error: {:?}", filter_nodes.err());
    }
}
