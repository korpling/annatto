use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    aql,
    graph::{AnnoKey, EdgeContainer, GraphStorage, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    dfs::{CycleSafeDFS, DFSStep},
    graph::{ANNIS_NS, NODE_NAME_KEY, storage::union::UnionEdgeContainer},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{manipulator::Manipulator, util::update_graph_silent};

/// This module can query annotations and create spans across
/// all matching nodes for the same value, adjacency is optional.
#[derive(Deserialize, Facet, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CreateSpans {
    /// The query for retrieving the relevant annotation values and
    /// nodes for the spans to be created.
    query: String,
    /// The node index (starting at 1) to pick the target node for the new span.
    /// Note, that the new span will not directly point to the target node, but
    /// will have edges of component `component` (s. below) to the covered tokens.
    node: usize,
    /// The annotation key holding the values on the newly created spans.
    #[serde(with = "crate::estarde::anno_key")]
    anno: AnnoKey,
    /// The query indices for determining an annotation value, join via empty string if
    /// more than one index is provided.
    value: Vec<usize>,
    /// By default only adjacent matches (in base ordering) will be covered by a new span.
    /// If discontinuous spans are legal or useful in your model, you can set this to `false`.
    #[serde(default = "default_adjacent")]
    adjacent: bool,
    /// The component for the spanning edges, by default `{ ctype = "Coverage", layer = "annis", name = ""}` (the default coverage component).
    #[serde(
        default = "default_component",
        with = "crate::estarde::annotation_component"
    )]
    component: AnnotationComponent,
}

fn default_adjacent() -> bool {
    true
}

fn default_component() -> AnnotationComponent {
    AnnotationComponent::new(
        AnnotationComponentType::Coverage,
        ANNIS_NS.to_string(),
        "".to_string(),
    )
}

impl Manipulator for CreateSpans {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        {
            let query = aql::parse(&self.query, false)?;
            let results = aql::execute_query_on_graph(graph, &query, true, None)?.flatten();
            let mut value_to_nodes = BTreeMap::default();
            let coverage_storages = graph
                .get_all_components(Some(AnnotationComponentType::Coverage), None)
                .into_iter()
                .flat_map(|c| graph.get_graphstorage(&c))
                .collect_vec();
            let coverage_container = UnionEdgeContainer::new(
                coverage_storages
                    .iter()
                    .map(|gs| gs.as_edgecontainer())
                    .collect_vec(),
            );
            let base_order_gs = graph
                .get_graphstorage(&AnnotationComponent::new(
                    AnnotationComponentType::Ordering,
                    ANNIS_NS.to_string(),
                    "".to_string(),
                ))
                .ok_or(anyhow!("Missing base ordering"))?;
            for match_group in results {
                let target_node = match_group
                    .get(self.node - 1)
                    .ok_or(anyhow!(
                        "Node index `{}` not compatible with query result length.",
                        self.node - 1
                    ))?
                    .node;
                let target_nodes =
                    CycleSafeDFS::new(&coverage_container, target_node, 0, usize::MAX)
                        .flatten()
                        .filter_map(|DFSStep { node, .. }| {
                            if let Ok(false) = coverage_container.has_outgoing_edges(node) {
                                Some(node)
                            } else {
                                None
                            }
                        })
                        .collect::<BTreeSet<NodeID>>();
                let identifying_value = {
                    let mut buffer = String::new();
                    self.value.iter().try_for_each(|i| {
                        let m = match_group
                            .get(i - 1)
                            .ok_or(anyhow!("Incompatible index for query."))?;
                        let v = graph
                            .get_node_annos()
                            .get_value_for_item(&m.node, &m.anno_key)?
                            .ok_or(anyhow!("No value for item."))?;
                        buffer.push_str(&v);
                        Ok::<(), anyhow::Error>(())
                    })?;
                    buffer
                };
                match value_to_nodes.entry(identifying_value) {
                    std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(target_nodes);
                    }
                    std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().extend(target_nodes);
                    }
                };
            }
            let part_of_storage = graph
                .get_graphstorage(&AnnotationComponent::new(
                    AnnotationComponentType::PartOf,
                    ANNIS_NS.to_string(),
                    "".to_string(),
                ))
                .ok_or(anyhow!("There is no default part of component."))?;
            for (anno_value, nodes) in value_to_nodes {
                let groups = if self.adjacent {
                    self.adjacent_groups(base_order_gs.clone(), nodes)?
                } else {
                    vec![nodes]
                };
                for (i, group) in groups.iter().enumerate() {
                    let parent_name = if let Some(elem) = group.first() {
                        let parent = part_of_storage
                            .find_connected(*elem, 1, std::ops::Bound::Included(1))
                            .next()
                            .ok_or(anyhow!("Invalid: Matching node has no parent."))??;
                        graph
                            .get_node_annos()
                            .get_value_for_item(&parent, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Parent has no name."))?
                    } else {
                        continue;
                    };
                    let span_name = format!(
                        "{parent_name}#merged_span_{}-{}-{anno_value}-{i}",
                        self.anno.ns, self.anno.name
                    );
                    update.add_event(UpdateEvent::AddNode {
                        node_name: span_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: span_name.to_string(),
                        anno_ns: self.anno.ns.to_string(),
                        anno_name: self.anno.name.to_string(),
                        anno_value: anno_value.to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: span_name.to_string(),
                        target_node: parent_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                    for target_node in group {
                        let target_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(target_node, &NODE_NAME_KEY)?
                            .ok_or(anyhow!("Node has no name."))?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: span_name.to_string(),
                            target_node: target_node_name.to_string(),
                            layer: self.component.layer.to_string(),
                            component_type: self.component.get_type().to_string(),
                            component_name: self.component.name.to_string(),
                        })?;
                    }
                }
            }
        }
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

impl CreateSpans {
    fn adjacent_groups(
        &self,
        ordering: Arc<dyn GraphStorage>,
        mut nodes: BTreeSet<NodeID>,
    ) -> Result<Vec<BTreeSet<NodeID>>, anyhow::Error> {
        let mut adjacent_groups = Vec::default();
        while let Some(node) = nodes.pop_last() {
            let mut connected = ordering
                .find_connected(node, 1, std::ops::Bound::Excluded(nodes.len()))
                .flatten()
                .collect::<BTreeSet<NodeID>>();
            connected.extend(
                ordering
                    .find_connected_inverse(node, 1, std::ops::Bound::Excluded(nodes.len()))
                    .flatten(),
            );
            let mut group = connected
                .intersection(&nodes)
                .copied()
                .collect::<BTreeSet<NodeID>>();
            group.insert(node);
            nodes = nodes.difference(&group).copied().collect();
            adjacent_groups.push(group);
        }
        Ok(adjacent_groups)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{AnnotationGraph, graph::AnnoKey, model::AnnotationComponent};
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{Importer, treetagger::ImportTreeTagger},
        manipulator::{
            Manipulator,
            span::{CreateSpans, default_component},
        },
        test_util::export_to_string,
    };

    #[test]
    fn deserialize() {
        let module: Result<CreateSpans, _> = toml::from_str(
            r#"
        query = "speaker _=_ tok"
        node = 2
        value = [1]
        anno = "speaker_span"        
        "#,
        );
        assert!(module.is_ok());
    }

    #[test]
    fn deserialize_custom() {
        let module: Result<CreateSpans, _> = toml::from_str(
            r#"
        adjacent = true
        query = "speaker _=_ tok"
        node = 2
        value = [1]
        anno = "speaker_span"
        component = { ctype = "Dominance", layer = "annis", name = "" }
        "#,
        );
        assert!(module.is_ok());
    }

    #[test]
    fn serialize_custom() {
        let module = CreateSpans {
            adjacent: true,
            query: "sentence".to_string(),
            node: 1,
            value: vec![1],
            anno: AnnoKey {
                name: "sentence_span".to_string(),
                ns: "".to_string(),
            },
            component: AnnotationComponent::new(
                graphannis::model::AnnotationComponentType::Dominance,
                ANNIS_NS.to_string(),
                "".to_string(),
            ),
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
    fn spans() {
        let importer: Result<ImportTreeTagger, _> =
            toml::from_str(r#"column_names = ["annis::tok", "sentence"]"#);
        assert!(importer.is_ok());
        let importer = importer.unwrap();
        let u = importer.import_corpus(
            Path::new("tests/data/graph_op/span/adjacent"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            importer.default_configuration(),
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let span_op = CreateSpans {
            adjacent: true,
            query: "sentence".to_string(),
            node: 1,
            value: vec![1],
            anno: AnnoKey {
                name: "sentence_span".to_string(),
                ns: "".to_string(),
            },
            component: default_component(),
        };
        let exec = span_op.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_spans".to_string(),
                path: None,
            },
            None,
        );
        assert!(exec.is_ok(), "Execution failed: {:?}", exec.err().unwrap());
        let exporter: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(exporter.is_ok());
        let actual = export_to_string(&graph, exporter.unwrap());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn discontinuous_spans() {
        let importer: Result<ImportTreeTagger, _> =
            toml::from_str(r#"column_names = ["annis::tok", "sentence"]"#);
        assert!(importer.is_ok());
        let importer = importer.unwrap();
        let u = importer.import_corpus(
            Path::new("tests/data/graph_op/span/non-adjacent"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            importer.default_configuration(),
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let span_op = CreateSpans {
            adjacent: false,
            query: "sentence".to_string(),
            node: 1,
            value: vec![1],
            anno: AnnoKey {
                name: "sentence_span".to_string(),
                ns: "".to_string(),
            },
            component: default_component(),
        };
        let exec = span_op.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_spans".to_string(),
                path: None,
            },
            None,
        );
        assert!(exec.is_ok(), "Execution failed: {:?}", exec.err().unwrap());
        let exporter: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(exporter.is_ok());
        let actual = export_to_string(&graph, exporter.unwrap());
        assert_snapshot!(actual.unwrap());
    }
}
