use std::collections::BTreeMap;

use anyhow::anyhow;
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, EdgeContainer, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    dfs::{CycleSafeDFS, DFSStep},
    graph::{ANNIS_NS, NODE_NAME_KEY, storage::union::UnionEdgeContainer},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{manipulator::Manipulator, progress::ProgressReporter, util::update_graph_silent};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DivideSegments {
    /// This determines which component provides the set of nodes whose values require a smaller division
    /// and in which component the divided nodes should be organized.
    /// These are usually two orderings with the minimal being the default ordering "Ordering/annis". If
    /// you want to use the default minimal you do not need to specify a value.
    ///
    /// Example:
    /// ```toml
    /// [[graph_op]]
    /// action = "divide"
    ///
    /// [graph_op.config.horizontal]
    /// source = { ctype = "Ordering", layer = "default_ns", name = "norm" }
    /// minimal = { ctype = "Ordering", layer = "annis", name = "" }
    /// ```
    horizontal: HorizontalTargets,
    #[serde(default)]
    vertical: VerticalTarget,
    #[serde(with = "crate::estarde::anno_key")]
    source_anno: AnnoKey,
    #[serde(with = "crate::estarde::anno_key", default = "default_target_anno")]
    target_anno: AnnoKey,
    #[serde(default)]
    op: DivideOp,
}

fn default_target_anno() -> AnnoKey {
    AnnoKey {
        name: "tok".to_string(),
        ns: ANNIS_NS.to_string(),
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
enum DivideOp {
    #[default]
    #[serde(rename = "char")]
    Char,
    #[serde(untagged)]
    Num {
        n: usize,
        #[serde(default = "default_segment_value")]
        value: String,
    },
}

fn default_segment_value() -> String {
    " ".to_string()
}

impl DivideOp {
    fn resolve(&self, value: &str) -> Vec<String> {
        match self {
            DivideOp::Char => value.chars().map(|c| c.to_string()).collect(),
            DivideOp::Num { n, value } => vec![value.to_string(); *n],
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HorizontalTargets {
    #[serde(with = "crate::estarde::annotation_component")]
    source: AnnotationComponent,
    #[serde(
        default = "default_minimal",
        with = "crate::estarde::annotation_component"
    )]
    minimal: AnnotationComponent,
}

fn default_minimal() -> AnnotationComponent {
    AnnotationComponent::new(
        AnnotationComponentType::Ordering,
        ANNIS_NS.to_string(),
        "".to_string(),
    )
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
enum VerticalTarget {
    Ctype(AnnotationComponentType),
    Components(
        #[serde(with = "crate::estarde::annotation_component::in_sequence")]
        Vec<AnnotationComponent>,
    ),
}

impl Default for VerticalTarget {
    fn default() -> Self {
        VerticalTarget::Ctype(AnnotationComponentType::Coverage)
    }
}

impl VerticalTarget {
    fn components(&self, graph: &AnnotationGraph) -> Vec<AnnotationComponent> {
        match self {
            VerticalTarget::Ctype(annotation_component_type) => {
                graph.get_all_components(Some(annotation_component_type.clone()), None)
            }
            VerticalTarget::Components(components) => components.clone(),
        }
    }
}

impl Manipulator for DivideSegments {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.horizontal.minimal == self.horizontal.source {
            return Err(anyhow!("Horizontal components need to be distinct.").into());
        }
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
        let mut update = GraphUpdate::default();
        {
            let source_gs = graph
                .get_graphstorage(&self.horizontal.source)
                .ok_or(anyhow!("No such component: {}", &self.horizontal.source))?;
            let source_node_sequences = {
                let roots = source_gs
                    .source_nodes()
                    .flatten()
                    .filter(|n| !source_gs.has_ingoing_edges(*n).unwrap_or_default());
                roots.map(|r| {
                    source_gs
                        .find_connected(r, 0, std::ops::Bound::Unbounded)
                        .flatten()
                })
            };

            graph.get_or_create_writable(&self.horizontal.minimal)?;
            let minimal_gs = graph
                .get_graphstorage(&self.horizontal.minimal)
                .ok_or(anyhow!("No such component: {}", &self.horizontal.minimal))?;
            let minimal_is_new = minimal_gs.as_edgecontainer().source_nodes().count() == 0;

            let vertical_gss = self
                .vertical
                .components(graph)
                .iter()
                .flat_map(|c| graph.get_graphstorage(c))
                .collect_vec();
            let vertical_container = UnionEdgeContainer::new(
                vertical_gss
                    .iter()
                    .map(|gs| gs.as_edgecontainer())
                    .collect_vec(),
            );

            let part_of_gs = graph
                .get_graphstorage(&AnnotationComponent::new(
                    AnnotationComponentType::PartOf,
                    ANNIS_NS.to_string(),
                    "".to_string(),
                ))
                .ok_or(anyhow!("There is no part of storage available."))?;

            let mut deleted_minimal_nodes: BTreeMap<NodeID, String> = BTreeMap::default();

            for node_sequence in source_node_sequences {
                let mut previous= None;
                for node in node_sequence {
                    let horizontal_node_name = graph
                        .get_node_annos()
                        .get_value_for_item(&node, &NODE_NAME_KEY)?
                        .unwrap_or_default();
                    let parent = part_of_gs
                        .find_connected(node, 1, std::ops::Bound::Included(1))
                        .next()
                        .ok_or(anyhow!(
                            "Node {horizontal_node_name} has no part of-parent."
                        ))??;
                    let parent_name = graph
                        .get_node_annos()
                        .get_value_for_item(&parent, &NODE_NAME_KEY)?
                        .ok_or(anyhow!("Parent has no name."))?;
                    let anno_value = graph
                        .get_node_annos()
                        .get_value_for_item(&node, &self.source_anno)?;
                    let node_name_stem = if let Some(frag) = horizontal_node_name.split("#").last()
                    {
                        frag.to_string()
                    } else {
                        chrono::Local::now().format("%M%S%9f").to_string()
                    };

                    if let Some(value) = &anno_value {
                        let new_values = self.op.resolve(value);
                        let names = new_values
                            .iter()
                            .enumerate()
                            .map(|(i, v)| format!("{parent_name}#divide_{node_name_stem}_{i}_{v}"));
                        let mut is_tok = false;
                        let (left_most, right_most) = if !vertical_container
                            .has_outgoing_edges(node)?
                        {
                            is_tok = true;
                            (node, node)
                        } else {
                            let vertically_reachable =
                                CycleSafeDFS::new(&vertical_container, node, 1, usize::MAX)
                                    .flatten()
                                    .filter_map(|DFSStep { node: n, .. }| {
                                        if !vertical_container
                                            .has_outgoing_edges(n)
                                            .unwrap_or_default()
                                            && (minimal_gs.has_ingoing_edges(n).unwrap_or_default()
                                                || minimal_gs
                                                    .has_outgoing_edges(n)
                                                    .unwrap_or_default())
                                        {
                                            Some(n)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect_vec();
                            let mut ordered_nodes = Vec::with_capacity(vertically_reachable.len());
                            let mut start_index: usize = 0;
                            let l = vertically_reachable.len();
                            while ordered_nodes.len() < l && start_index < l {
                                ordered_nodes.clear();
                                let start_node = vertically_reachable[start_index];
                                minimal_gs
                                    .find_connected(start_node, 0, std::ops::Bound::Excluded(l))
                                    .flatten()
                                    .filter(|n| vertically_reachable.contains(n))
                                    .for_each(|n| ordered_nodes.push(n));
                                start_index += 1;
                            }
                            if ordered_nodes.len() < l {
                                return Err(anyhow!(
                                "Could not obtain ordered minimal nodes from vertical container."
                            )
                            .into());
                            }
                            (ordered_nodes[0], ordered_nodes[ordered_nodes.len() - 1])
                        };
                        if left_most != right_most {
                            // problematic case, especially in "char" mode
                            return Err(anyhow!(
                            "This graph op currently does not support the provided graph structure."
                        )
                        .into());
                        } else {
                            previous = if minimal_is_new && let Some(prev_id) = previous {
                                Some(prev_id)
                            } else if let Some(prev_id) = minimal_gs
                                .find_connected_inverse(left_most, 1, std::ops::Bound::Included(1))
                                .flatten()
                                .next()
                            {
                                deleted_minimal_nodes
                                    .get(&prev_id)
                                    .map(String::to_string)
                                    .or(graph
                                        .get_node_annos()
                                        .get_value_for_item(&prev_id, &NODE_NAME_KEY)?
                                        .map(|v| v.to_string()))
                            } else {
                                None
                            };
                            for (new_node, new_value) in names.zip_eq(&new_values) {
                                update.add_event(UpdateEvent::AddNode {
                                    node_name: new_node.to_string(),
                                    node_type: "node".to_string(),
                                })?;
                                if let Some(prev_name) = previous {
                                    update.add_event(UpdateEvent::AddEdge {
                                        source_node: prev_name,
                                        target_node: new_node.to_string(),
                                        layer: self.horizontal.minimal.layer.to_string(),
                                        component_type: self
                                            .horizontal
                                            .minimal
                                            .get_type()
                                            .to_string(),
                                        component_name: self.horizontal.minimal.name.to_string(),
                                    })?;
                                }
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: new_node.to_string(),
                                    target_node: parent_name.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::PartOf.to_string(),
                                    component_name: "".to_string(),
                                })?;
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: new_node.to_string(),
                                    anno_ns: self.target_anno.ns.to_string(),
                                    anno_name: self.target_anno.name.to_string(),
                                    anno_value: new_value.to_string(),
                                })?;
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: horizontal_node_name.to_string(),
                                    target_node: new_node.to_string(),
                                    layer: ANNIS_NS.to_string(),
                                    component_type: AnnotationComponentType::Coverage.to_string(),
                                    component_name: "".to_string(),
                                })?;
                                previous = Some(new_node);
                            }
                            if let Some(name) = &previous
                                && !is_tok
                            {
                                update.add_event(UpdateEvent::DeleteNode {
                                    node_name: graph
                                        .get_node_annos()
                                        .get_value_for_item(&left_most, &NODE_NAME_KEY)?
                                        .ok_or(anyhow!("No has no name."))?
                                        .to_string(),
                                })?;
                                deleted_minimal_nodes.insert(right_most, name.to_string());
                                // just in case the successor of left_most (== right_most) prevails,
                                // it needs to be integrated.
                                // If it gets deleted in the process, the edge will be, too
                                if let Some(successor_id) = minimal_gs
                                    .find_connected(left_most, 1, std::ops::Bound::Included(1))
                                    .flatten()
                                    .next()
                                {
                                    let successor_name = graph
                                        .get_node_annos()
                                        .get_value_for_item(&successor_id, &NODE_NAME_KEY)?
                                        .ok_or(anyhow!("Node has no name"))?
                                        .to_string();
                                    update.add_event(UpdateEvent::AddEdge {
                                        source_node: name.to_string(),
                                        target_node: successor_name,
                                        layer: self.horizontal.minimal.layer.to_string(),
                                        component_type: self
                                            .horizontal
                                            .minimal
                                            .get_type()
                                            .to_string(),
                                        component_name: self.horizontal.minimal.name.to_string(),
                                    })?;
                                }
                            }
                        }
                    } else {
                        progress.warn(format!(
                            "Source node {horizontal_node_name} has no value for key {}:{}",
                            self.source_anno.ns, self.source_anno.name
                        ))?;
                        continue;
                    }
                }
            }
        }
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{Importer, treetagger::ImportTreeTagger, xlsx::ImportSpreadsheet},
        manipulator::{Manipulator, divide::DivideSegments},
        test_util::export_to_string,
    };

    #[test]
    fn single_tok() {
        let import: Result<ImportTreeTagger, _> =
            toml::from_str(r#"column_names = ["annis::tok", "default_ns::pos"]"#);
        assert!(import.is_ok());
        let import = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/divide/single-tok/"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            import.default_configuration(),
            None,
        );
        assert!(u.is_ok());
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let manip: Result<DivideSegments, _> = toml::from_str(
            r#"
        source_anno = "annis::tok"
        op = "char"

        [horizontal]
        source = { ctype = "Ordering", layer = "annis", name = "" }
        minimal = { ctype = "Ordering", layer = "annis", name = "new" }
        "#,
        );
        assert!(
            manip.is_ok(),
            "Err deserializing: {:?}",
            manip.err().unwrap()
        );
        let manip = manip.unwrap();
        let appl = manip.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_divide".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            appl.is_ok(),
            "Error performing divide: {:?}",
            appl.err().unwrap()
        );
        let exporter: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(exporter.is_ok());
        let exporter = exporter.unwrap();
        assert_snapshot!(export_to_string(&graph, exporter).unwrap());
    }

    #[test]
    fn offset_tok() {
        let import: Result<ImportSpreadsheet, _> = toml::from_str(
            r#"
            [column_map]
            norm = ["pos", "lemma"]
            "#,
        );
        assert!(import.is_ok());
        let import = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/divide/offset-tok/"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            import.default_configuration(),
            None,
        );
        assert!(u.is_ok());
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let manip: Result<DivideSegments, _> = toml::from_str(
            r#"
        source_anno = "norm::norm"
        op = "char"

        [horizontal]
        source = { ctype = "Ordering", layer = "default_ns", name = "norm" }
        minimal = { ctype = "Ordering", layer = "annis", name = "" }
        "#,
        );
        assert!(
            manip.is_ok(),
            "Err deserializing: {:?}",
            manip.err().unwrap()
        );
        let manip = manip.unwrap();
        let appl = manip.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_divide".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            appl.is_ok(),
            "Error performing divide: {:?}",
            appl.err().unwrap()
        );
        let exporter: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(exporter.is_ok());
        let exporter = exporter.unwrap();
        assert_snapshot!(export_to_string(&graph, exporter).unwrap());
    }

    #[test]
    fn multi_tok_err() {
        let import: Result<ImportSpreadsheet, _> = toml::from_str(
            r#"
            [column_map]
            dipl = ["sentence", "seg"]
            norm = ["pos", "lemma"]
            "#,
        );
        assert!(import.is_ok());
        let import = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/graph_op/divide/multi-tok/"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            import.default_configuration(),
            None,
        );
        assert!(u.is_ok());
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let manip: Result<DivideSegments, _> = toml::from_str(
            r#"
        source_anno = "norm::norm"
        op = "char"

        [horizontal]
        source = { ctype = "Ordering", layer = "default_ns", name = "norm" }
        minimal = { ctype = "Ordering", layer = "annis", name = "" }
        "#,
        );
        assert!(
            manip.is_ok(),
            "Err deserializing: {:?}",
            manip.err().unwrap()
        );
        let manip = manip.unwrap();
        let appl = manip.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_divide".to_string(),
                path: None,
            },
            None,
        );
        assert!(appl.is_err());
    }
}
