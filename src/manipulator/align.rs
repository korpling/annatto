use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use facet::Facet;
use graphannis::{
    AnnotationGraph, aql,
    graph::NodeID,
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    progress::ProgressReporter,
    util::update_graph_silent,
    util::{
        sort_matches::SortCache,
        token_helper::{self, TokenHelper},
    },
};

use super::Manipulator;

/// Aligns nodes identified by queries with edges in the defined component.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
pub struct AlignNodes {
    /// Define node groups that should be aligned. Neighbouring node groups in the
    /// provided list are aligned, given common nodes can be identified. You can
    /// define more than two node groups.
    ///
    /// Example:
    ///
    /// ```toml
    /// [[graph_op]]
    /// action = "align"
    ///
    /// [[graph_op.config.groups]]
    /// query = "norm @* doc"
    /// link = 1
    /// groupby = 2
    ///
    /// [[graph_op.config.groups]]
    /// query = "tok!=/ / @* doc"
    /// link = 1
    /// groupby = 2
    /// ```
    ///
    /// The example links nodes with a `norm` annotation. It groups them by document name.
    /// The nodes are aligned with `tok` nodes, also grouped by document names, which need
    /// to be identical to the first group's document names to have them aligned.
    ///
    groups: Vec<NodeGroup>,
    #[serde(
        with = "crate::estarde::annotation_component",
        default = "default_component"
    )]
    /// This defines the component within which the alignment edges are created. The default
    /// value is `{ ctype = "Pointing", layer = "", name = "align" }`.
    ///
    /// Example:
    ///
    /// ```toml
    /// [graph_op.config]
    /// component = { ctype = "Pointing", layer = "", name = "align" }
    /// ```
    component: AnnotationComponent,
    /// Select an alignment method. Currently only `ses` is supported, but in the future
    /// other methods might be available. Therefore this does not need to, but can be set.
    #[serde(default)]
    method: AlignmentMethod,
}

fn default_component() -> AnnotationComponent {
    AnnotationComponent::new(AnnotationComponentType::Pointing, "".into(), "align".into())
}

impl Manipulator for AlignNodes {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let data = self.identify(graph)?;
        let progress = ProgressReporter::new(tx, step_id, data.len() - 1)?;
        let aligner = match self.method {
            AlignmentMethod::Ses => SESAligner {
                graph,
                component: self.component.clone(),
                progress,
            },
        };
        for (source_bundles, target_bundles) in data.into_iter().tuple_windows() {
            aligner.align(&mut update, source_bundles, target_bundles)?;
        }
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
struct NodeGroup {
    query: String,
    link: usize,
    groupby: usize,
}

#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
enum AlignmentMethod {
    #[default]
    Ses,
}

type ValueNodeMap = BTreeMap<String, Vec<(NodeID, String)>>;

impl AlignNodes {
    fn identify(&self, graph: &AnnotationGraph) -> Result<Vec<ValueNodeMap>, anyhow::Error> {
        let mut groups = Vec::with_capacity(self.groups.len());
        let gs_order = graph.get_graphstorage(&AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        ));
        let mut sort_cache = SortCache::new(gs_order.context("Missing ordering component")?);
        let token_helper = TokenHelper::new(graph)?;
        for node_group in &self.groups {
            let mut nodes_by_value = ValueNodeMap::default();
            let query = aql::parse(&node_group.query, false)?;
            let mut result = aql::execute_query_on_graph(graph, &query, true, None)?
                .flatten()
                .collect_vec();
            result.sort_by(|m1, m2| {
                sort_cache
                    .compare_matchgroup_by_text_pos(m1, m2, graph.get_node_annos(), &token_helper)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            for match_group in result {
                if let Some(link_node_data) = match_group.get(node_group.link - 1) {
                    let link_node_key = if link_node_data.anno_key == *NODE_TYPE_KEY {
                        &token_helper::TOKEN_KEY
                    } else {
                        &link_node_data.anno_key
                    };
                    if let Some(node_data) = match_group.get(node_group.groupby - 1) {
                        let value_key = if node_data.anno_key == *NODE_TYPE_KEY {
                            &token_helper::TOKEN_KEY
                        } else {
                            &node_data.anno_key
                        };
                        if let Some(identifying_value) = graph
                            .get_node_annos()
                            .get_value_for_item(&node_data.node, value_key)?
                        {
                            let data = (
                                link_node_data.node,
                                graph
                                    .get_node_annos()
                                    .get_value_for_item(&link_node_data.node, link_node_key)?
                                    .unwrap_or_default()
                                    .to_string(),
                            );
                            match nodes_by_value.entry(identifying_value.to_string()) {
                                std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                                    vacant_entry.insert(vec![data]);
                                }
                                std::collections::btree_map::Entry::Occupied(
                                    mut occupied_entry,
                                ) => {
                                    occupied_entry.get_mut().push(data);
                                }
                            }
                        }
                    }
                }
            }
            groups.push(nodes_by_value);
        }
        Ok(groups)
    }
}

struct SESAligner<'a> {
    graph: &'a AnnotationGraph,
    component: AnnotationComponent,
    progress: ProgressReporter,
}

impl<'a> SESAligner<'a> {
    fn align(
        &'a self,
        update: &mut GraphUpdate,
        source_bundles: ValueNodeMap,
        target_bundles: ValueNodeMap,
    ) -> Result<(), anyhow::Error> {
        for (group_id, source_entries) in source_bundles {
            if let Some(target_entries) = target_bundles.get(&group_id) {
                let source_values = source_entries.iter().map(|(_, s)| s.as_str()).collect_vec();
                let target_values = target_entries.iter().map(|(_, s)| s.as_str()).collect_vec();
                let (alignments, _) = tokenizations::get_alignments(&source_values, &target_values);
                for (src_index, tgt_indices) in alignments.iter().enumerate() {
                    if let Some((source_node_id, _)) = source_entries.get(src_index) {
                        let source_node_name = self
                            .graph
                            .get_node_annos()
                            .get_value_for_item(source_node_id, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node {} has no name.", source_node_id))?;
                        for tgt_index in tgt_indices {
                            if let Some((target_node_id, _)) = target_entries.get(*tgt_index) {
                                let target_node_name = self
                                    .graph
                                    .get_node_annos()
                                    .get_value_for_item(target_node_id, &NODE_NAME_KEY)?
                                    .ok_or(anyhow!("Node {} has no name.", source_node_id))?;
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: source_node_name.to_string(),
                                    target_node: target_node_name.to_string(),
                                    layer: self.component.layer.to_string(),
                                    component_type: self.component.get_type().to_string(),
                                    component_name: self.component.name.to_string(),
                                })?;
                            }
                        }
                    }
                }
            }
        }
        self.progress.worked(1)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{AnnotationGraph, update::GraphUpdate};
    use insta::assert_snapshot;

    use crate::{
        StepID,
        exporter::graphml::GraphMLExporter,
        importer::{Importer, treetagger::ImportTreeTagger},
        manipulator::{
            Manipulator,
            align::{AlignNodes, AlignmentMethod, NodeGroup, default_component},
        },
        test_util::export_to_string,
        util::example_generator,
        util::update_graph_silent,
    };

    #[test]
    fn serialize() {
        let module = AlignNodes {
            groups: vec![
                NodeGroup {
                    query: "norm @* doc".to_string(),
                    link: 1,
                    groupby: 2,
                },
                NodeGroup {
                    query: "tok @* doc".to_string(),
                    link: 1,
                    groupby: 2,
                },
            ],
            component: default_component(),
            method: AlignmentMethod::default(),
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
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = AlignNodes {
            groups: vec![
                NodeGroup {
                    query: "norm @* doc".to_string(),
                    link: 1,
                    groupby: 2,
                },
                NodeGroup {
                    query: "tok @* doc".to_string(),
                    link: 1,
                    groupby: 2,
                },
            ],
            component: default_component(),
            method: AlignmentMethod::default(),
        };
        assert!(
            module
                .validate_graph(
                    &mut graph,
                    StepID {
                        module_name: "test".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        assert!(graph.global_statistics.is_some());
    }

    #[test]
    fn align() {
        let module: Result<ImportTreeTagger, _> = toml::from_str("column_names = [\"annis::tok\"]");
        assert!(module.is_ok());
        let import = module.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/align/"),
            crate::StepID {
                module_name: "import_docs".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut update = u.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let graph_op: Result<AlignNodes, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "align" }
        [[groups]]
        query = "tok @* doc @* node_name=/.*a/"
        groupby = 2
        link = 1

        [[groups]]
        query = "tok @* doc @* node_name=/.*b/"
        groupby = 2
        link = 1
        "#,
        );
        assert!(
            graph_op.is_ok(),
            "Error deserializing: {:?}",
            graph_op.err()
        );
        assert!(
            graph_op
                .unwrap()
                .manipulate_corpus(
                    &mut graph,
                    Path::new("./"),
                    crate::StepID {
                        module_name: "test_align".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        let to_graphml = export_to_string(&graph, GraphMLExporter::default());
        assert!(to_graphml.is_ok());
        assert_snapshot!(to_graphml.unwrap());
    }
}
