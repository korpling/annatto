use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, Edge, NodeID},
    model::AnnotationComponent,
    update::{GraphUpdate, UpdateEvent},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    manipulator::Manipulator,
    progress::ProgressReporter,
    util::{node_name, update_graph_silent},
};

/// Annotation can be moved from edges of a component
/// to the source or target node, but also from nodes
/// to edges going out of or into the carrying node.
///
/// The following moves annotations from the edge to the
/// target node of the edge:
/// ```toml
/// [graph_op.config]
/// component = { ctype = "Pointing", layer = "", name = "dep" }
/// anno = "default_ns::deprel"
/// direction = "target"
/// ```
///
/// Moving to the target is the default and does not need
/// to be explicated.
///
/// This moves an annotation from nodes to ingoing edges:
/// ```toml
/// [graph_op.config]
/// component = { ctype = "Pointing", layer = "", name = "dep" }
/// anno = "default_ns::pos"
/// direction = "in"
/// ```
#[derive(Clone, Deserialize, Facet, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MoveAnnos {
    /// The annotation component the involved edges are
    /// contained in.
    #[serde(with = "crate::estarde::annotation_component")]
    component: AnnotationComponent,
    /// The annotation key of the annotation to be moved.
    #[serde(with = "crate::estarde::anno_key")]
    anno: AnnoKey,
    /// The direction of move. Potential values are "source",
    /// "target", "in", and "out".
    #[serde(default)]
    direction: MoveDirection,
    /// Setting this to `true` keeps the original annotation.
    /// Default is `false`.
    #[serde(default)]
    copy: bool,
    /// In case that a node (only for directions `source` and `target`)
    /// receives multiple annotations, this case needs to be dealt with.
    /// Mode "naive" (default) ignores and potentially overwrites annotations
    /// created earlier in the process. Providing a delimiter joins all applicable
    /// values:
    /// ```toml
    /// [graph_op.config]
    /// multi = { delimiter = "," }
    /// ```
    ///
    /// Instead of joining nodes, they can also be distributed across multiple
    /// annotations on the same node. In this case, the namespace will be
    /// used as an index. You thus lose control over the maximal index used,
    /// but you can still retrieve annotations with the bare annotation
    /// name (e. g. for deletion down the line):
    /// ```toml
    /// [graph_op.config]
    /// multi = "index"
    /// ```
    /// Note that index mode leads to loss of the namespace for all annotations,
    /// i. e., nodes, that only carry one value, will still have namespace "0"
    /// for their annotation.
    ///
    #[serde(default)]
    multi: MultiValueMode,
}

#[derive(Clone, Default, Deserialize, Facet, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
enum MoveDirection {
    Source,
    #[default]
    Target,
    In,
    Out,
}

#[derive(Clone, Default, Deserialize, Facet, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
enum MultiValueMode {
    Delimiter(String),
    Index,
    #[default]
    Naive,
}

impl Manipulator for MoveAnnos {
    fn manipulate_corpus(
        &self,
        graph: &mut AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let gs = graph
            .get_graphstorage_as_ref(&self.component)
            .ok_or(anyhow!("Could not get component storage."))?;
        let mut node_values: BTreeMap<NodeID, BTreeSet<Cow<str>>> = BTreeMap::default();
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
        if matches!(&self.multi, MultiValueMode::Index)
            && matches!(&self.direction, MoveDirection::In | MoveDirection::Out)
        {
            progress.warn(
                "You are using multi value mode `index` with an edge-related move direction. 
            Edges can never inherit a value from more than one source or target node. 
            The multi value mode will thus be ignored.",
            )?;
        }
        for source in gs.source_nodes().flatten() {
            for target in gs
                .find_connected(source, 1, std::ops::Bound::Included(1))
                .flatten()
            {
                let source_name = node_name(graph, source)?;
                let target_name = node_name(graph, target)?;
                match &self.direction {
                    MoveDirection::Source | MoveDirection::Target => {
                        if let Some(anno_value) = gs
                            .get_anno_storage()
                            .get_value_for_item(&Edge { source, target }, &self.anno)?
                        {
                            if !self.copy {
                                update.add_event(UpdateEvent::DeleteEdgeLabel {
                                    source_node: source_name.to_string(),
                                    target_node: target_name.to_string(),
                                    layer: self.component.layer.to_string(),
                                    component_type: self.component.get_type().to_string(),
                                    component_name: self.component.name.to_string(),
                                    anno_ns: self.anno.ns.to_string(),
                                    anno_name: self.anno.name.to_string(),
                                })?;
                            }
                            let insert_node = if matches!(&self.direction, MoveDirection::Source) {
                                source
                            } else {
                                target
                            };
                            // for this case we need to collect for later concatenation / listing
                            node_values
                                .entry(insert_node)
                                .or_default()
                                .insert(anno_value);
                        }
                    }
                    MoveDirection::In | MoveDirection::Out => {
                        if !self.copy {
                            let node_of_interest = if matches!(&self.direction, MoveDirection::In) {
                                target
                            } else {
                                source
                            };
                            if let Some(anno_value) = graph
                                .get_node_annos()
                                .get_value_for_item(&node_of_interest, &self.anno)?
                            {
                                if !self.copy {
                                    update.add_event(UpdateEvent::DeleteNodeLabel {
                                        node_name: node_name(graph, node_of_interest)?.to_string(),
                                        anno_ns: self.anno.ns.to_string(),
                                        anno_name: self.anno.name.to_string(),
                                    })?;
                                }
                                update.add_event(UpdateEvent::AddEdgeLabel {
                                    source_node: source_name.to_string(),
                                    target_node: target_name.to_string(),
                                    layer: self.component.layer.to_string(),
                                    component_type: self.component.get_type().to_string(),
                                    component_name: self.component.name.to_string(),
                                    anno_ns: self.anno.ns.to_string(),
                                    anno_name: self.anno.name.to_string(),
                                    anno_value: anno_value.to_string(),
                                })?;
                            }
                        }
                    }
                };
            }
        }
        for (node, values) in node_values {
            let node_name = node_name(graph, node)?;
            match &self.multi {
                MultiValueMode::Delimiter(delim) => {
                    let joint_value = values.iter().join(delim);
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: self.anno.ns.to_string(),
                        anno_name: self.anno.name.to_string(),
                        anno_value: joint_value,
                    })?;
                }
                MultiValueMode::Index => {
                    for (index, value) in values.iter().enumerate() {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: index.to_string(),
                            anno_name: self.anno.name.to_string(),
                            anno_value: value.to_string(),
                        })?;
                    }
                }
                MultiValueMode::Naive => {
                    for value in values {
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: self.anno.ns.to_string(),
                            anno_name: self.anno.name.to_string(),
                            anno_value: value.to_string(),
                        })?;
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

    use graphannis::{
        AnnotationGraph,
        errors::GraphAnnisError,
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        manipulator::{Manipulator, mv::MoveAnnos},
        test_util::export_to_string,
    };

    #[test]
    fn naive_in() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "in"
        multi = "naive"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn naive_out() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "out"
        multi = "naive"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn naive_source() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "source"
        multi = "naive"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn naive_target() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "target"
        multi = "naive"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn index_in() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "in"
        multi = "index"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn index_out() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "out"
        multi = "index"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn index_source() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "source"
        multi = "index"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn index_target() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "target"
        multi = "index"
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn delim_in() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "in"
        multi = { delimiter = "," }
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn delim_out() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::lemma"
        direction = "out"
        multi = { delimiter = "," }
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn delim_source() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "source"
        multi = { delimiter = "," }
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    #[test]
    fn delim_target() {
        let g = test_graph();
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let m: Result<MoveAnnos, _> = toml::from_str(
            r#"
        component = { ctype = "Pointing", layer = "", name = "ref" }
        anno = "default_ns::ref_type"
        direction = "target"
        multi = { delimiter = "," }
        "#,
        );
        assert!(m.is_ok(), "Err deserializing: {:?}", m.err().unwrap());
        let module = m.unwrap();
        let exec = module.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            crate::StepID {
                module_name: "test_manipulation".to_string(),
                path: None,
            },
            None,
        );
        assert!(
            exec.is_ok(),
            "Err executing move: {:?}",
            exec.err().unwrap()
        );
        let graphml_export: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        assert_snapshot!(export_to_string(&graph, graphml_export).unwrap());
    }

    fn test_graph() -> Result<AnnotationGraph, GraphAnnisError> {
        let mut graph = AnnotationGraph::with_default_graphstorages(false)?;
        let mut update = GraphUpdate::default();
        update.add_event(UpdateEvent::AddNode {
            node_name: "corpus".to_string(),
            node_type: "corpus".to_string(),
        })?;
        let data = [
            ("This", "this", 4, "anaphoric"),
            ("is", "be", 0, ""),
            ("the", "the", 4, "coref"),
            ("test", "test", 0, ""),
        ];
        for i in 1..data.len() + 1 {
            let name = format!("corpus#t{i}");
            update.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: name,
                target_node: "corpus".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
        }
        for (i, (tok, lemma, points_to, edge_value)) in data.iter().enumerate() {
            let index = i + 1;
            let source = format!("corpus#t{index}");
            let target = format!("corpus#t{points_to}");
            if *points_to > 0i32 {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: source.to_string(),
                    target_node: target.to_string(),
                    layer: "".to_string(),
                    component_type: AnnotationComponentType::Pointing.to_string(),
                    component_name: "ref".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdgeLabel {
                    source_node: source.to_string(),
                    target_node: target.to_string(),
                    layer: "".to_string(),
                    component_type: AnnotationComponentType::Pointing.to_string(),
                    component_name: "ref".to_string(),
                    anno_ns: "default_ns".to_string(),
                    anno_name: "ref_type".to_string(),
                    anno_value: edge_value.to_string(),
                })?;
            }
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: source.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: tok.to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: source.to_string(),
                anno_ns: "default_ns".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma.to_string(),
            })?;
        }
        graph.apply_update(&mut update, |_| {})?;
        Ok(graph)
    }
}
