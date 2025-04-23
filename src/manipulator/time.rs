use std::{collections::BTreeMap, ops::Bound};

use anyhow::{anyhow, bail};
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, EdgeContainer, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::graph::{storage::union::UnionEdgeContainer, ANNIS_NS, NODE_NAME_KEY};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{core::update_graph_silent, progress::ProgressReporter};

use super::Manipulator;

/// This module adds time values to all nodes of type `node` in a graph. It either fills gaps in time values as long
/// as the start and end of an ordering have defined values, or it adds time values from 0 to the number of ordered
/// nodes in the case that absolutely no time values exist yet. In all other cases it will fail. Time values are
/// interpolated along ordering edges and propagated along coverage edges.
///
/// Example:
/// ```toml
/// [[graph_op]]
/// action = "time"
///
/// [graph_op.config]
/// ```
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Filltime {
    /// A fallback start time in case it cannot be derived.
    ///
    /// Example:
    /// ```toml
    /// [[graph_op]]
    /// action = "time"
    ///
    /// [graph_op.config]
    /// fallback_start = 0.0
    /// ```
    #[serde(default)]
    fallback_start: Option<f64>,
}

impl Manipulator for Filltime {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let roots = {
            let base_ordering_storage = graph
                .get_graphstorage(&AnnotationComponent::new(
                    AnnotationComponentType::Ordering,
                    ANNIS_NS.into(),
                    "".into(),
                ))
                .ok_or(anyhow!("Base ordering storage unavailable."))?;
            base_ordering_storage
                .source_nodes()
                .flatten()
                .filter(|n| {
                    !base_ordering_storage
                        .has_ingoing_edges(*n)
                        .unwrap_or_default()
                })
                .collect_vec()
        };
        let mut update = GraphUpdate::default();
        let mut node_to_start = BTreeMap::default();
        let mut node_to_end = BTreeMap::default();
        for m in graph
            .get_node_annos()
            .exact_anno_search(
                Some(ANNIS_NS),
                "time",
                graphannis_core::annostorage::ValueSearch::Any,
            )
            .flatten()
        {
            let node = m.node;
            if let Some(value) = graph
                .get_node_annos()
                .get_value_for_item(&node, &m.anno_key)?
            {
                if let Some((start_s, end_s)) = value.split_once('-') {
                    if !start_s.is_empty() {
                        node_to_start.insert(node, start_s.parse::<f64>()?.into());
                    };
                    if !end_s.is_empty() {
                        node_to_end.insert(node, end_s.parse::<f64>()?.into());
                    };
                }
            }
        }
        let progress = ProgressReporter::new(tx, step_id, roots.len())?;
        for root in roots {
            self.fill(
                graph,
                &mut update,
                root,
                &mut node_to_start,
                &mut node_to_end,
                &progress,
            )?;
            progress.worked(1)?;
        }
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

impl Filltime {
    fn fill(
        &self,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
        start_node: NodeID,
        start_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
        end_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
        progress: &ProgressReporter,
    ) -> Result<(), anyhow::Error> {
        // spread existing values along coverage edges
        lr_propagate(graph, start_cache, end_cache)?;
        // check ordering for non-timed nodes and if necessary, interpolate
        order_interpolate(
            graph,
            start_node,
            start_cache,
            end_cache,
            self.fallback_start,
        )?;
        // do l-r propagation a second time
        lr_propagate(graph, start_cache, end_cache)?;
        // build update
        let time_key = AnnoKey {
            ns: ANNIS_NS.into(),
            name: "time".into(),
        };
        for (node, start_time) in start_cache {
            let node_name = graph
                .get_node_annos()
                .get_value_for_item(node, &NODE_NAME_KEY)?
                .ok_or(anyhow!("Node has no name."))?;
            if let Some(end_time) = end_cache.get(node) {
                if !graph
                    .get_node_annos()
                    .has_value_for_item(node, &time_key)
                    .unwrap_or_default()
                {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: time_key.ns.to_string(),
                        anno_name: time_key.name.to_string(),
                        anno_value: format!("{:.16}-{:.16}", start_time, end_time),
                    })?;
                }
            } else {
                progress.warn(format!("Node {node_name} could not be assigned a time annotation as there is no end time available.").as_str())?;
            }
        }
        Ok(())
    }
}

fn interpolate(
    start_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
    end_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
    target_nodes: &mut Vec<u64>,
    lower: OrderedFloat<f64>,
    upper: OrderedFloat<f64>,
) {
    let start_values = (1..target_nodes.len() + 1)
        .map(|i| {
            (upper - lower)
                * (OrderedFloat::from(i as f64) / OrderedFloat::from(target_nodes.len() as f64))
                + lower
        })
        .collect_vec();
    for (n, st) in target_nodes.iter().zip(start_values.iter()) {
        end_cache.insert(*n, *st);
    }
    for (n, et) in target_nodes.iter().skip(1).zip(start_values.iter()) {
        start_cache.insert(*n, *et);
    }
    target_nodes.clear();
}

fn order_interpolate(
    graph: &AnnotationGraph,
    start_node: NodeID,
    start_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
    end_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
    fallback: Option<f64>,
) -> Result<(), anyhow::Error> {
    let ordering_storage = graph
        .get_graphstorage(&AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        ))
        .ok_or(anyhow!("Ordering storage unavailable."))?;
    let ordered_nodes = ordering_storage
        .find_connected(start_node, 0, Bound::Unbounded)
        .flatten()
        .collect_vec();
    let has_time_values = ordered_nodes
        .iter()
        .any(|n| start_cache.contains_key(n) || end_cache.contains_key(n));
    if !has_time_values {
        if let Some(first_node) = ordered_nodes.first() {
            start_cache.insert(*first_node, OrderedFloat::from(0));
            end_cache.insert(*first_node, OrderedFloat::from(1));
        }
        if let Some(last_node) = ordered_nodes.last() {
            start_cache.insert(
                *last_node,
                OrderedFloat::from((ordered_nodes.len() - 1) as f64),
            );
            end_cache.insert(*last_node, OrderedFloat::from(ordered_nodes.len() as f64));
        }
    }
    let mut last_known_time = if let Some(et) =
        end_cache.get(&start_node).copied().map(|o| *o).or(fallback)
    {
        OrderedFloat::from(et)
    } else {
        bail!("Could not determine start time value to initiate interpolation. Consider setting a fallback value.")
    };
    let mut untimed_nodes = Vec::new();
    for node in ordered_nodes {
        if untimed_nodes.is_empty() {
            start_cache.entry(node).or_insert(last_known_time);
            if let Some(t) = end_cache.get(&node) {
                last_known_time = *t;
            } else {
                untimed_nodes.push(node);
            }
        } else if let Some(t) = start_cache.remove(&node) {
            interpolate(
                start_cache,
                end_cache,
                &mut untimed_nodes,
                last_known_time,
                t,
            );
            last_known_time = t;
            start_cache.insert(node, t);
        } else if let Some(t) = end_cache.remove(&node) {
            untimed_nodes.push(node);
            interpolate(
                start_cache,
                end_cache,
                &mut untimed_nodes,
                last_known_time,
                t,
            );
            last_known_time = t;
            end_cache.insert(node, t);
        } else {
            untimed_nodes.push(node);
        }
    }
    Ok(())
}

fn lr_propagate(
    graph: &AnnotationGraph,
    start_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
    end_cache: &mut BTreeMap<NodeID, OrderedFloat<f64>>,
) -> Result<(), anyhow::Error> {
    let coverage_storages = graph
        .get_all_components(Some(AnnotationComponentType::Coverage), None)
        .iter()
        .flat_map(|c| {
            graph.get_graphstorage(c).ok_or(anyhow!(
                "Storage of coverage component {}::{} unavailable",
                c.layer,
                c.name
            ))
        })
        .collect_vec();
    let coverage_container = UnionEdgeContainer::new(
        coverage_storages
            .iter()
            .map(|s| s.as_edgecontainer())
            .collect_vec(),
    );
    let l_storage = graph
        .get_graphstorage(&AnnotationComponent::new(
            AnnotationComponentType::LeftToken,
            ANNIS_NS.into(),
            "".into(),
        ))
        .ok_or(anyhow!("Left-token storage unavailable."))?;
    let r_storage = graph
        .get_graphstorage(&AnnotationComponent::new(
            AnnotationComponentType::RightToken,
            ANNIS_NS.into(),
            "".into(),
        ))
        .ok_or(anyhow!("Right-token storage unavailable."))?;
    let mut terminated = false;
    while !terminated {
        let mut inherited_start = BTreeMap::default();
        let mut inherited_end = BTreeMap::default();
        for (host_node, start_value) in start_cache.iter() {
            if coverage_container.has_outgoing_edges(*host_node)? {
                // not a token, i. e. a source node in l/r
                if let Some(tok) = l_storage
                    .find_connected(*host_node, 1, Bound::Included(1))
                    .flatten()
                    .next()
                {
                    if !start_cache.contains_key(&tok) {
                        inherited_start.insert(tok, *start_value);
                    }
                }
                if let Some(tok) = r_storage
                    .find_connected(*host_node, 1, Bound::Included(1))
                    .flatten()
                    .next()
                {
                    if !end_cache.contains_key(&tok) {
                        if let Some(end_value) = end_cache.get(host_node) {
                            inherited_end.insert(tok, *end_value);
                        }
                    }
                }
            } else if coverage_container.has_ingoing_edges(*host_node)? {
                // a token, i. e. a target node in l/r
                for incoming_from in l_storage.get_ingoing_edges(*host_node).flatten() {
                    if !start_cache.contains_key(&incoming_from) {
                        inherited_start.insert(incoming_from, *start_value);
                    }
                }
                for incoming_from in r_storage.get_ingoing_edges(*host_node).flatten() {
                    if let Some(end_value) = end_cache.get(host_node) {
                        if !end_cache.contains_key(&incoming_from) {
                            inherited_end.insert(incoming_from, *end_value);
                        }
                    }
                }
            }
        }
        if inherited_start.is_empty() && inherited_end.is_empty() {
            terminated = true;
        }
        start_cache.extend(inherited_start);
        end_cache.extend(inherited_end);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{update::GraphUpdate, AnnotationGraph};
    use insta::assert_snapshot;

    use crate::{
        core::update_graph_silent, exporter::graphml::GraphMLExporter, importer::{conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, Importer}, manipulator::{time::Filltime, Manipulator}, test_util::export_to_string, util::example_generator, StepID
    };

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = Filltime::default();
        assert!(module
            .validate_graph(
                &mut graph,
                StepID {
                    module_name: "test".to_string(),
                    path: None
                },
                None
            )
            .is_ok());
        assert!(graph.global_statistics.is_none());
    }

    #[test]
    fn sparse_to_full_fail() {
        let import_exmaralda = ImportEXMARaLDA::default();
        let import = import_exmaralda.import_corpus(
            Path::new("./tests/data/import/exmaralda/valid-sparse-timevalues/"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(import.is_ok(), "import failed: {:?}", import.err());
        let mut update = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let apply_update = graph.apply_update(&mut update, |_| {});
        assert!(
            apply_update.is_ok(),
            "Applying update failed: {:?}",
            apply_update.err()
        );
        let manipulate = Filltime::default();
        let fill_time = manipulate.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_fill_time".to_string(),
                path: None,
            },
            None,
        );
        assert!(fill_time.is_err());
    }

    #[test]
    fn with_fallback() {
        let import_exmaralda = ImportEXMARaLDA::default();
        let import = import_exmaralda.import_corpus(
            Path::new("./tests/data/import/exmaralda/valid-sparse-timevalues/"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(import.is_ok(), "import failed: {:?}", import.err());
        let mut update = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let apply_update = graph.apply_update(&mut update, |_| {});
        assert!(
            apply_update.is_ok(),
            "Applying update failed: {:?}",
            apply_update.err()
        );
        let mnp: Result<Filltime, _> = toml::from_str("fallback_start = 0.0");
        assert!(mnp.is_ok(), "Error deserializing: {:?}", mnp.err());
        let manipulate = mnp.unwrap();
        let fill_time = manipulate.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_fill_time".to_string(),
                path: None,
            },
            None,
        );
        assert!(fill_time.is_ok());
    }

    #[test]
    fn sparse_to_full_pass() {
        let import_exmaralda = ImportEXMARaLDA::default();
        let import = import_exmaralda.import_corpus(
            Path::new("./tests/data/import/exmaralda/valid-sparse-timevalues_2/"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(import.is_ok(), "import failed: {:?}", import.err());
        let mut update = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let apply_update = graph.apply_update(&mut update, |_| {});
        assert!(
            apply_update.is_ok(),
            "Applying update failed: {:?}",
            apply_update.err()
        );
        let manipulate = Filltime::default();
        let fill_time = manipulate.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_fill_time".to_string(),
                path: None,
            },
            None,
        );
        assert!(fill_time.is_ok(), "Error occured: {:?}", fill_time.err());
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok(), "Export failed: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn none_to_full() {
        let import_conll = ImportCoNLLU::default();
        let import = import_conll.import_corpus(
            Path::new("./tests/data/import/conll/valid/"),
            StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(import.is_ok(), "import failed: {:?}", import.err());
        let mut update = import.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let apply_update = graph.apply_update(&mut update, |_| {});
        assert!(
            apply_update.is_ok(),
            "Applying update failed: {:?}",
            apply_update.err()
        );
        let manipulate = Filltime::default();
        let fill_time = manipulate.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_fill_time".to_string(),
                path: None,
            },
            None,
        );
        assert!(fill_time.is_ok(), "Error occured: {:?}", fill_time.err());
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok(), "Export failed: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }
}
