use anyhow::{anyhow, Result};
use graphannis::{graph::GraphStorage, model::AnnotationComponentType, AnnotationGraph};
use graphannis_core::{
    annostorage::{NodeAnnotationStorage, ValueSearch},
    graph::ANNIS_NS,
    types::{AnnoKey, Component, NodeID},
};

use lazy_static::lazy_static;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

#[derive(Clone)]
pub struct TokenHelper<'a> {
    node_annos: &'a dyn NodeAnnotationStorage,
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

        Ok(TokenHelper {
            node_annos: graph.get_node_annos(),
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
        for n in self
            .node_annos
            .exact_anno_search(Some(ANNIS_NS), "tok", ValueSearch::Any)
        {
            let n = n?;

            // Check that this is an actual token and there are no outgoing coverage edges
            if self.is_token(n.node)? {
                if ordering_gs.get_ingoing_edges(n.node).next().is_none() {
                    roots.insert(n.node);
                }
            }
        }

        // Filter the roots by checking the parent node in the corpus structure
        let mut roots_for_document = Vec::new();
        if let Some(parent_id) = parent_id {
            for n in roots {
                if self
                    .part_of_gs
                    .is_connected(n, parent_id, 1, std::ops::Bound::Unbounded)?
                {
                    roots_for_document.push(n);
                }
            }
        }

        // Follow the ordering edges from the roots to reconstruct the token in their correct order
        let mut result = Vec::default();
        for r in roots_for_document {
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

    pub fn spanned_text(&self, token_id: NodeID) -> Result<Cow<str>> {
        let anno_value = self.node_annos.get_value_for_item(&token_id, &TOKEN_KEY)?;
        let result = anno_value.unwrap_or_default();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use graphannis::{update::GraphUpdate, AnnotationGraph};
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use crate::util::example_generator;

    use super::TokenHelper;

    #[test]
    fn example_graph_token() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let token_helper = TokenHelper::new(&g).unwrap();

        let ordered_token_ids = token_helper
            .get_ordered_token("root/doc1", None)
            .unwrap()
            .into_iter()
            .map(|t_id| token_helper.spanned_text(t_id).unwrap())
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
}
