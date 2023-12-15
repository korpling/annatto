use graphannis::{graph::GraphStorage, model::AnnotationComponentType, AnnotationGraph};
use graphannis_core::{
    annostorage::NodeAnnotationStorage,
    errors::GraphAnnisCoreError,
    graph::ANNIS_NS,
    types::{AnnoKey, Component, NodeID},
};

use lazy_static::lazy_static;
use std::sync::Arc;

#[derive(Clone)]
pub struct TokenHelper<'a> {
    node_annos: &'a dyn NodeAnnotationStorage,
    left_edges: Arc<dyn GraphStorage>,
    right_edges: Arc<dyn GraphStorage>,
    cov_edges: Vec<Arc<dyn GraphStorage>>,
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

        let left_edges = graph
            .get_graphstorage(&COMPONENT_LEFT)
            .ok_or_else(|| GraphAnnisCoreError::MissingComponent(COMPONENT_LEFT.to_string()))?;
        let right_edges = graph
            .get_graphstorage(&COMPONENT_RIGHT)
            .ok_or_else(|| GraphAnnisCoreError::MissingComponent(COMPONENT_RIGHT.to_string()))?;

        Ok(TokenHelper {
            node_annos: graph.get_node_annos(),
            left_edges,
            right_edges,
            cov_edges,
        })
    }
    pub fn get_gs_coverage(&self) -> &Vec<Arc<dyn GraphStorage>> {
        &self.cov_edges
    }

    pub fn get_gs_left_token(&self) -> &dyn GraphStorage {
        self.left_edges.as_ref()
    }

    pub fn get_gs_right_token(&self) -> &dyn GraphStorage {
        self.right_edges.as_ref()
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
}
