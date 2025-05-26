use anyhow::{anyhow, Result};
use graphannis::{graph::GraphStorage, model::AnnotationComponentType, AnnotationGraph};
use graphannis_core::{
    annostorage::NodeAnnotationStorage,
    dfs,
    errors::GraphAnnisCoreError,
    graph::{storage::union::UnionEdgeContainer, ANNIS_NS},
    types::{AnnoKey, Component, NodeID},
};

use itertools::Itertools;
use lazy_static::lazy_static;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

#[derive(Clone)]
pub struct TokenHelper<'a> {
    node_annos: &'a dyn NodeAnnotationStorage,
    left_edges: Arc<dyn GraphStorage>,
    cov_edges: Vec<Arc<dyn GraphStorage>>,
    ordering_gs: BTreeMap<String, Arc<dyn GraphStorage>>,
    part_of_gs: Arc<dyn GraphStorage>,
}

lazy_static! {
    static ref COMPONENT_LEFT: Component<AnnotationComponentType> = {
        Component::new(
            AnnotationComponentType::LeftToken,
            ANNIS_NS.into(),
            "".into(),
        )
    };
    static ref COMPONENT_RIGHT: Component<AnnotationComponentType> = {
        Component::new(
            AnnotationComponentType::RightToken,
            ANNIS_NS.into(),
            "".into(),
        )
    };
    pub static ref TOKEN_KEY: Arc<AnnoKey> = Arc::from(AnnoKey {
        ns: ANNIS_NS.into(),
        name: "tok".into(),
    });
}

impl<'a> TokenHelper<'a> {
    pub fn new(graph: &'a AnnotationGraph) -> anyhow::Result<TokenHelper<'a>> {
        let cov_edges: Vec<Arc<dyn GraphStorage>> = graph
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .into_iter()
            .filter_map(|c| graph.get_graphstorage(&c))
            .filter(|gs| {
                if let Some(stats) = gs.get_statistics() {
                    stats.nodes > 0
                } else {
                    true
                }
            })
            .collect();

        let mut ordering_gs = BTreeMap::new();

        for c in graph.get_all_components(Some(AnnotationComponentType::Ordering), None) {
            if let Some(gs) = graph.get_graphstorage(&c) {
                ordering_gs.insert(c.name.to_string(), gs);
            }
        }

        let part_of_component =
            Component::new(AnnotationComponentType::PartOf, ANNIS_NS.into(), "".into());
        let part_of_gs = graph
            .get_graphstorage(&part_of_component)
            .ok_or_else(|| anyhow!("Missing PartOf component"))?;

        let left_edges = graph
            .get_graphstorage(&COMPONENT_LEFT)
            .ok_or_else(|| GraphAnnisCoreError::MissingComponent(COMPONENT_LEFT.to_string()))?;

        Ok(TokenHelper {
            node_annos: graph.get_node_annos(),
            left_edges,
            cov_edges,
            ordering_gs,
            part_of_gs,
        })
    }
    pub fn get_gs_coverage(&self) -> &Vec<Arc<dyn GraphStorage>> {
        &self.cov_edges
    }

    pub fn is_token(&self, id: NodeID) -> anyhow::Result<bool> {
        if self.node_annos.has_value_for_item(&id, &TOKEN_KEY)? {
            // check if there is no outgoing edge in any of the coverage components
            let has_outgoing = self.has_outgoing_coverage_edges(id)?;
            Ok(!has_outgoing)
        } else {
            Ok(false)
        }
    }

    pub fn has_outgoing_coverage_edges(&self, id: NodeID) -> anyhow::Result<bool> {
        for c in self.cov_edges.iter() {
            if c.has_outgoing_edges(id)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn get_ordered_token(
        &self,
        parent_name: &str,
        segmentation: Option<&str>,
    ) -> Result<Vec<NodeID>> {
        let parent_id = self.node_annos.get_node_id_from_name(parent_name)?;
        let segmentation = segmentation.unwrap_or("");
        let ordering_gs = &self
            .ordering_gs
            .get(segmentation)
            .ok_or_else(|| anyhow!("Missing ordering component for segmentation {segmentation}"))?;

        // Find all token roots
        let mut roots: HashSet<_> = HashSet::new();
        for n in ordering_gs.source_nodes() {
            let n = n?;
            if !ordering_gs.has_ingoing_edges(n)? {
                // Filter the roots by checking the parent node in the corpus structure
                if let Some(parent_id) = parent_id {
                    if self
                        .part_of_gs
                        .is_connected(n, parent_id, 1, std::ops::Bound::Unbounded)?
                    {
                        roots.insert(n);
                    }
                } else {
                    roots.insert(n);
                }
            }
        }

        // Follow the ordering edges from the roots to reconstruct the token in their correct order
        let mut result = Vec::default();
        for r in roots {
            let mut token = Some(r);
            while let Some(current_token) = token {
                result.push(current_token);
                // Get next token
                if let Some(next_token) = ordering_gs.get_outgoing_edges(current_token).next() {
                    let next_token = next_token?;
                    token = Some(next_token);
                } else {
                    token = None;
                }
            }
        }

        Ok(result)
    }

    pub fn spanned_text(&self, token_ids: &[NodeID]) -> Result<String> {
        let anno_values: std::result::Result<Vec<_>, GraphAnnisCoreError> = token_ids
            .iter()
            .map(|t| self.node_annos.get_value_for_item(t, &TOKEN_KEY))
            .collect();
        // TODO: support whitespace after/before annotations
        let anno_values = anno_values?.into_iter().flatten().collect_vec();
        let result = anno_values.join(" ");
        Ok(result)
    }

    /// Find all token covered by the given node
    pub fn covered_token(&self, node_id: NodeID) -> Result<Vec<NodeID>> {
        let mut result = Vec::default();
        let coverage = UnionEdgeContainer::new(
            self.cov_edges
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect_vec(),
        );
        let it = dfs::CycleSafeDFS::new(&coverage, node_id, 1, usize::MAX);
        for step in it {
            let step = step?;
            if self.is_token(step.node)? {
                result.push(step.node);
            }
        }

        // Sort token by their order
        if let Some(gs) = self.ordering_gs.get("") {
            result.sort_by(|a, b| {
                if a == b {
                    Ordering::Equal
                } else if let Ok(connected) = gs.is_connected(*a, *b, 1, std::ops::Bound::Unbounded)
                {
                    if connected {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Less
                }
            });
        }

        Ok(result)
    }

    pub fn left_token_for(&self, n: NodeID) -> Result<Option<NodeID>> {
        if self.is_token(n)? {
            Ok(Some(n))
        } else {
            let mut out = self.left_edges.get_outgoing_edges(n);
            match out.next() {
                Some(out) => Ok(Some(out?)),
                None => Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use crate::util::example_generator;

    use super::TokenHelper;

    #[test]
    fn example_graph_token() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let token_helper = TokenHelper::new(&g).unwrap();

        let ordered_token_ids = token_helper
            .get_ordered_token("root/doc1", None)
            .unwrap()
            .into_iter()
            .map(|t_id| token_helper.spanned_text(&[t_id]).unwrap())
            .collect_vec();

        assert_eq!(
            vec![
                "Is",
                "this",
                "example",
                "more",
                "complicated",
                "than",
                "it",
                "appears",
                "to",
                "be",
                "?"
            ],
            ordered_token_ids
        );
    }

    #[test]
    fn ordered_token_with_segmentation() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));

        // Add an additional segmentation layer
        example_generator::make_span(
            &mut updates,
            "root/doc1#seg1",
            &["root/doc1#tok1", "root/doc1#tok2", "root/doc1#tok3"],
            true,
        );
        updates
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "root/doc1#seg1".into(),
                anno_ns: ANNIS_NS.into(),
                anno_name: "tok".into(),
                anno_value: "This".into(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg1".into(),
                target_node: "root/doc1".into(),
                layer: ANNIS_NS.into(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".into(),
            })
            .unwrap();

        example_generator::make_span(&mut updates, "root/doc1#seg2", &["root/doc1#tok4"], true);
        updates
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "root/doc1#seg2".into(),
                anno_ns: ANNIS_NS.into(),
                anno_name: "tok".into(),
                anno_value: "more".into(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg2".into(),
                target_node: "root/doc1".into(),
                layer: ANNIS_NS.into(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".into(),
            })
            .unwrap();

        example_generator::make_span(&mut updates, "root/doc1#seg3", &["root/doc1#tok5"], true);
        updates
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "root/doc1#seg3".into(),
                anno_ns: ANNIS_NS.into(),
                anno_name: "tok".into(),
                anno_value: "complicated".into(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg3".into(),
                target_node: "root/doc1".into(),
                layer: ANNIS_NS.into(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".into(),
            })
            .unwrap();

        // add the order relations for the segmentation
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg1".into(),
                target_node: "root/doc1#seg2".into(),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "seg".to_string(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg2".into(),
                target_node: "root/doc1#seg3".into(),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "seg".to_string(),
            })
            .unwrap();

        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let token_helper = TokenHelper::new(&g).unwrap();

        let ordered_token_ids = token_helper
            .get_ordered_token("root/doc1", Some("seg"))
            .unwrap()
            .into_iter()
            .map(|t_id| token_helper.spanned_text(&[t_id]).unwrap())
            .collect_vec();

        assert_eq!(vec!["This", "more", "complicated",], ordered_token_ids);
    }
}
