//! Created edges between nodes based on their annotation value.
use super::Manipulator;
use crate::{
    StepID, core::update_graph_silent, error::AnnattoError, progress::ProgressReporter,
    workflow::StatusSender,
};
use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    AnnotationGraph, aql,
    graph::NodeID,
    model::AnnotationComponent,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{graph::NODE_NAME_KEY, types::AnnoKey};
use itertools::Itertools;
use serde::Serialize;
use serde_derive::Deserialize;
use std::{collections::BTreeMap, ops::Deref};
use struct_field_names_as_array::FieldNamesAsSlice;

/// Link nodes within a graph. Source and target of a link are determined via
/// queries; type, layer, and name of the link component can be configured.
///
/// This manipulator collects a source node set and a target node set given
/// the respective queries. In each node set, the nodes are mapped to a value.
/// Between each node in the source node set and the target node set, that are
/// assigned the same value, an edge is created (from source to target node).
/// The edges will be part of the defined component. Additionally, annotations
/// can be moved from the source or target node onto the edge.
///
/// The values assigned to each node in the source or target node set can be
/// created in several ways:
/// - a value from a single node in the query
/// - a concatenated value from multiple nodes in the query
/// - a concatenated value using a delimiter (`value_sep`) from multiple nodes in the query
///
/// The value formation is the crucial part of building correct edges.
///
/// Example:
/// ```toml
/// [[graph_op]]
/// action = "link"
///
/// [graph_op.config]
/// source_query = "tok _=_ id @* doc"
/// source_node = 1
/// source_value = [3, 2]
/// target_query = "func _=_ norm _=_ norm_id @* doc"
/// target_node = 2
/// target_value = [4, 3]
/// target_to_edge = [1]
/// component = { ctype = "Pointing", layer = "", name = "align" }
/// value_sep = "-"
/// ```
///
/// The example builds the source node set by trying to find all tok-nodes that have an id
/// and are linked to a node with a `doc` annotation (the document name) via a PartOf edge.
/// As source node, that goes into the said, the first (`1`) node from each result is
/// chosen, i. e. the token. The value, that is used to find a mapping partner from the
/// target node set, is build with the third (`3`) and second (`2`) node, concatenated
/// by a dash (s. `value_sep`). So a token with id "7", which is part of "document1",
/// will be assigned the value "document1-7".
///
/// The target configuration of query, node, and value maps nodes with a norm (`2`)
/// annotation to values, that concatenate the document name and the `norm_id`
/// annotation via a dash. So a norm token with id "7" in document "document1" will
/// also be assigned the value "document1-7".
///
/// This leads to edges from tokens with the same value to norm nodes with the same
/// value within the graphANNIS component `Pointing//align`.
///
/// Additionally, all edges are assigned a func annotation retrieved in the target query,
/// as `target_to_edge` is configured to copy annotation "1", which is `func` in the
/// example query, to the edge.
///
#[derive(
    Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize, Clone, PartialEq,
)]
#[serde(deny_unknown_fields)]
pub struct LinkNodes {
    /// The AQL query to find all source node annotations. Source and target nodes are then paired by equal value for their query match.
    source_query: String,
    /// The 1-based index selecting the value providing node in the AQL source query.
    source_node: usize,
    /// Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.
    source_value: Vec<usize>,
    /// This 1-based index list can be used to copy the given annotations from the source query to the edge that is to be created.
    #[serde(default)]
    source_to_edge: Vec<usize>,
    /// The AQL query to find all target node annotations.
    target_query: String,
    /// The 1-based index selecting the value providing node in the AQL target query.
    target_node: usize,
    /// Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.
    target_value: Vec<usize>,
    /// This 1-based index list can be used to copy the given annotations from the target query to the edge that is to be created.
    #[serde(default)]
    target_to_edge: Vec<usize>,
    /// The edge component to be built.
    #[serde(with = "crate::estarde::annotation_component")]
    component: AnnotationComponent,
    /// In case of multiple `source_values` or `target_values` this delimiter (default empty string) will be used for value concatenation.
    #[serde(default)]
    value_sep: String,
}

impl Manipulator for LinkNodes {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let link_sources = gather_link_data(
            graph,
            self.source_query.to_string(),
            self.source_node,
            &self.source_value,
            &self.source_to_edge,
            &self.value_sep,
            &step_id,
        )?;
        let link_targets = gather_link_data(
            graph,
            self.target_query.to_string(),
            self.target_node,
            &self.target_value,
            &self.target_to_edge,
            &self.value_sep,
            &step_id,
        )?;
        let mut update = self.link_nodes(link_sources, link_targets, tx, step_id)?;
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

type NodeBundle = Vec<(AnnoKey, NodeID)>;

/// This function executes a single query and returns bundled results or an error.
/// A bundled result is the annotation key the node has a match for and the matching node itself.
fn retrieve_nodes_with_values(
    graph: &AnnotationGraph,
    query: String,
) -> Result<Vec<NodeBundle>, Box<dyn std::error::Error>> {
    let mut node_bundles = Vec::new();
    let disj = aql::parse(&query, false)?;

    for m in aql::execute_query_on_graph(graph, &disj, true, None)?.flatten() {
        node_bundles.push(
            m.into_iter()
                .map(|match_member| (match_member.anno_key.deref().clone(), match_member.node))
                .collect_vec(),
        );
    }
    Ok(node_bundles)
}

type NodeNameWithEdgeData = (String, Vec<(AnnoKey, String)>);

/// This function queries the corpus graph and returns the relevant match data.
/// The returned data maps an annotation value or a joint value (value) to the nodes holding said value.
fn gather_link_data(
    graph: &AnnotationGraph,
    query: String,
    node_index: usize,
    value_indices: &[usize],
    edge_indices: &[usize],
    sep: &str,
    step_id: &StepID,
) -> Result<BTreeMap<String, Vec<NodeNameWithEdgeData>>, Box<dyn std::error::Error>> {
    let mut data: BTreeMap<String, Vec<NodeNameWithEdgeData>> = BTreeMap::new();
    let node_annos = graph.get_node_annos();
    for group_of_bundles in retrieve_nodes_with_values(graph, query.to_string())? {
        if let Some((_, link_node_id)) = group_of_bundles.get(node_index - 1) {
            let mut target_data = Vec::new();
            let mut value_segments = Vec::new();
            let mut edge_data = Vec::new();
            for edge_index in edge_indices {
                if let Some((k, n)) = group_of_bundles.get(edge_index - 1)
                    && let Some(v) = graph.get_node_annos().get_value_for_item(n, k)?
                {
                    edge_data.push(((*k).clone(), v.to_string()));
                }
            }
            for value_index in value_indices {
                if let Some((anno_key, value_node_id)) = group_of_bundles.get(*value_index - 1) {
                    if let Some(anno_value) =
                        node_annos.get_value_for_item(value_node_id, anno_key)?
                    {
                        value_segments.push(anno_value.trim().to_lowercase()); // simply concatenate values
                    }
                } else {
                    return Err(AnnattoError::Manipulator {
                        reason: format!(
                            "Could not extract node with value index {value_index} from query `{}`",
                            &query
                        ),
                        manipulator: step_id.module_name.to_string(),
                    }
                    .into());
                }
                let link_node_name = graph
                    .get_node_annos()
                    .get_value_for_item(link_node_id, &NODE_NAME_KEY)?
                    .ok_or(anyhow!("Could not determine node name."))?
                    .to_string();
                target_data.push((link_node_name, edge_data.clone())); // memory-inefficient for large queries, but that should usually not happen
            }
            let joint_value = value_segments.join(sep);
            if let Some(nodes_with_value) = data.get_mut(&joint_value) {
                nodes_with_value.extend(target_data);
            } else {
                data.insert(joint_value, target_data);
            }
        } else {
            return Err(AnnattoError::Manipulator {
                reason: format!(
                    "Could not extract node with node index {node_index} from query `{}`",
                    &query
                ),
                manipulator: step_id.module_name.to_string(),
            }
            .into());
        }
    }
    Ok(data)
}

impl LinkNodes {
    fn link_nodes(
        &self,
        sources: BTreeMap<String, Vec<NodeNameWithEdgeData>>,
        targets: BTreeMap<String, Vec<NodeNameWithEdgeData>>,
        tx: Option<StatusSender>,
        step_id: StepID,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let progress = ProgressReporter::new(tx, step_id, sources.len())?;
        for (anno_value, node_list) in sources {
            if let Some(target_node_list) = targets.get(&anno_value) {
                for ((source, src_edge_data), (target, tgt_edge_data)) in
                    node_list.iter().cartesian_product(target_node_list)
                {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        layer: self.component.layer.to_string(),
                        component_type: self.component.get_type().to_string(),
                        component_name: self.component.name.to_string(),
                    })?;
                    for data in [src_edge_data, tgt_edge_data] {
                        for (k, v) in data {
                            update.add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source.to_string(),
                                target_node: target.to_string(),
                                layer: self.component.layer.to_string(),
                                component_type: self.component.get_type().to_string(),
                                component_name: self.component.name.to_string(),
                                anno_ns: k.ns.to_string(),
                                anno_name: k.name.to_string(),
                                anno_value: v.to_string(),
                            })?;
                        }
                    }
                }
            }
            progress.worked(1)?;
        }
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{
        AnnotationGraph,
        model::{AnnotationComponent, AnnotationComponentType},
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        StepID,
        core::update_graph_silent,
        exporter::graphml::GraphMLExporter,
        manipulator::{Manipulator, link::LinkNodes},
        test_util::export_to_string,
        util::example_generator,
    };

    #[test]
    fn serialize_custom() {
        let module = LinkNodes {
            source_query: "node @* doc=/1/".to_string(),
            source_node: 1,
            source_value: vec![1],
            source_to_edge: vec![],
            target_query: "node @* doc=/2/".to_string(),
            target_node: 1,
            target_value: vec![1],
            target_to_edge: vec![],
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "link".into(),
            ),
            value_sep: "#".to_string(),
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
        let module = LinkNodes {
            source_query: "node".to_string(),
            source_node: 1,
            source_value: vec![],
            source_to_edge: vec![],
            target_query: "node".to_string(),
            target_node: 1,
            target_value: vec![],
            target_to_edge: vec![],
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "link".into(),
            ),
            value_sep: "".to_string(),
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
    fn link() -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = source_graph()?;
        let linker = LinkNodes {
            source_query: "norm _=_ lemma".to_string(),
            source_node: 1,
            source_value: vec![2],
            target_query: "morph & node? !> #1".to_string(),
            target_node: 1,
            target_value: vec![1],
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "morphology".into(),
            ),
            value_sep: "".to_string(),
            source_to_edge: vec![2],
            target_to_edge: vec![1],
        };
        linker.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_linker".into(),
                path: None,
            },
            None,
        )?;
        let actual = export_to_string(&graph, GraphMLExporter::default())?;
        assert_snapshot!(actual);
        Ok(())
    }

    fn source_graph() -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        // copied this from exmaralda test
        let mut graph = AnnotationGraph::with_default_graphstorages(true)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "import".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/exmaralda".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda".to_string(),
            target_node: "import".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/exmaralda/test_doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda/test_doc".to_string(),
            target_node: "import/exmaralda".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let tlis = ["T286", "T0", "T1", "T2", "T3", "T4"];
        let times = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        for tli in tlis {
            let node_name = format!("import/exmaralda/test_doc#{}", tli);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
        }
        for window in tlis.windows(2) {
            let tli0 = window[0];
            let tli1 = window[1];
            let source = format!("import/exmaralda/test_doc#{}", tli0);
            let target = format!("import/exmaralda/test_doc#{}", tli1);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source,
                target_node: target,
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
        let mut prev: Option<String> = None;
        for (tpe, spk, name, value, start, end, reset_after) in [
            ("t", "dipl", "dipl", "I'm", 0, 2, false),
            ("t", "dipl", "dipl", "in", 2, 3, false),
            ("t", "dipl", "dipl", "New", 3, 4, false),
            ("t", "dipl", "dipl", "York", 4, 5, true),
            ("a", "dipl", "sentence", "1", 0, 5, true),
            ("t", "norm", "norm", "I", 0, 1, false),
            ("t", "norm", "norm", "am", 1, 2, false),
            ("t", "norm", "norm", "in", 2, 3, false),
            ("t", "norm", "norm", "New York", 3, 5, true),
            ("a", "norm", "lemma", "I", 0, 1, true),
            ("a", "norm", "lemma", "be", 1, 2, true),
            ("a", "norm", "lemma", "in", 2, 3, true),
            ("a", "norm", "lemma", "New York", 3, 5, true),
            ("a", "norm", "pos", "PRON", 0, 1, true),
            ("a", "norm", "pos", "VERB", 1, 2, true),
            ("a", "norm", "pos", "ADP", 2, 3, true),
            ("a", "norm", "pos", "PROPN", 3, 5, true),
        ] {
            let node_name = format!(
                "{}#{}_{}_{}-{}",
                "import/exmaralda/test_doc", tpe, spk, tlis[start], tlis[end]
            );
            let start_time = times[start];
            let end_time = times[end];
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "time".to_string(),
                anno_value: format!("{}-{}", start_time, end_time),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: spk.to_string(),
            })?;
            if tpe == "t" {
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: value.to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: "import/exmaralda/test_doc".to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                if let Some(other_name) = prev {
                    u.add_event(UpdateEvent::AddEdge {
                        source_node: other_name,
                        target_node: node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: spk.to_string(),
                    })?;
                }
                prev = if reset_after {
                    None
                } else {
                    Some(node_name.to_string())
                }
            }
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: spk.to_string(),
                anno_name: name.to_string(),
                anno_value: value.to_string(),
            })?;
            for i in start..end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: format!("import/exmaralda/test_doc#{}", tlis[i]),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // add unlinked corpus nodes
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/i".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex".to_string(),
            target_node: "import".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york".to_string(),
            target_node: "import/lex".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/i".to_string(),
            target_node: "import/lex".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        // add unlinked data nodes
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#root".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#root".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "New York".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#m1".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#m1".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "New".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#m2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#m2".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "York".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york#m1".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york#m2".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/i#root".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/i#root".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "I".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/i#root".to_string(),
            target_node: "import/lex/i".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        graph.apply_update(&mut u, |_| {})?;
        Ok(graph)
    }
}
