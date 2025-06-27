use anyhow::Result;
use graphannis::graph::Match;
use graphannis_core::annostorage::NodeAnnotationStorage;
use graphannis_core::graph::NODE_NAME_KEY;
use graphannis_core::{graph::storage::GraphStorage, types::NodeID};
use lru::LruCache;
use nonzero::nonzero;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;

use super::token_helper::TokenHelper;

pub(crate) struct SortCache {
    node_name: LruCache<NodeID, String>,
    left_token: LruCache<NodeID, Option<NodeID>>,
    is_connected: LruCache<(NodeID, NodeID), bool>,
    gs_order: Arc<dyn GraphStorage>,
}

impl SortCache {
    pub fn new(gs_order: Arc<dyn GraphStorage>) -> Self {
        Self {
            node_name: LruCache::new(nonzero!(1000usize)),
            left_token: LruCache::new(nonzero!(1000usize)),
            is_connected: LruCache::new(nonzero!(1000usize)),
            gs_order,
        }
    }

    pub fn compare_matchgroup_by_text_pos(
        &mut self,
        m1: &[Match],
        m2: &[Match],
        node_annos: &dyn NodeAnnotationStorage,
        token_helper: &TokenHelper,
    ) -> Result<Ordering> {
        for i in 0..std::cmp::min(m1.len(), m2.len()) {
            let element_cmp =
                self.compare_match_by_text_pos(&m1[i], &m2[i], node_annos, token_helper)?;
            if element_cmp != Ordering::Equal {
                return Ok(element_cmp);
            }
        }
        // Sort longer vectors ("more specific") before shorter ones
        // This originates from the old SQL based system, where an "unfilled" match position had the NULL value.
        // NULL values where sorted *after* the ones with actual values. In practice, this means the more specific
        // matches come first.
        Ok(m2.len().cmp(&m1.len()))
    }

    pub fn compare_match_by_text_pos(
        &mut self,
        m1: &Match,
        m2: &Match,
        node_annos: &dyn NodeAnnotationStorage,
        token_helper: &TokenHelper,
    ) -> Result<Ordering> {
        if m1.node == m2.node {
            // same node, use annotation name and namespace to compare
            Ok(m1.anno_key.cmp(&m2.anno_key))
        } else {
            // get the node paths and names

            let m1_anno_val = if let Some(val) = self.node_name.get(&m1.node) {
                Some(Cow::Owned(val.clone()))
            } else {
                let val = node_annos.get_value_for_item(&m1.node, &NODE_NAME_KEY)?;
                if let Some(val) = &val {
                    self.node_name.put(m1.node, val.to_string());
                }
                val
            };

            let m2_anno_val = if let Some(val) = self.node_name.get(&m2.node) {
                Some(Cow::Borrowed(val.as_str()))
            } else {
                let val = node_annos.get_value_for_item(&m2.node, &NODE_NAME_KEY)?;
                if let Some(val) = &val {
                    self.node_name.put(m2.node, val.to_string());
                }
                val
            };

            if let Some(m1_anno_val) = m1_anno_val
                && let Some(m2_anno_val) = m2_anno_val
            {
                let (m1_path, m1_name) = split_path_and_nodename(&m1_anno_val);
                let (m2_path, m2_name) = split_path_and_nodename(&m2_anno_val);

                // 1. compare the path
                let path_cmp = compare_document_path(m1_path, m2_path);
                if path_cmp != Ordering::Equal {
                    return Ok(path_cmp);
                }

                // 2. compare the token ordering
                // Try to get left token from cache

                let m1_lefttok = if let Some(lefttok) = self.left_token.get(&m1.node).copied() {
                    lefttok
                } else {
                    let result = token_helper.left_token_for(m1.node)?;
                    self.left_token.put(m1.node, result);
                    result
                };

                let m2_lefttok = if let Some(lefttok) = self.left_token.get(&m2.node).copied() {
                    lefttok
                } else {
                    let result = token_helper.left_token_for(m2.node)?;
                    self.left_token.put(m2.node, result);
                    result
                };

                if let Some(m1_lefttok) = m1_lefttok
                    && let Some(m2_lefttok) = m2_lefttok
                {
                    let token_are_connected =
                        if let Some(v) = self.is_connected.get(&(m1_lefttok, m2_lefttok)) {
                            *v
                        } else {
                            self.gs_order.is_connected(
                                m1_lefttok,
                                m2_lefttok,
                                1,
                                std::ops::Bound::Unbounded,
                            )?
                        };

                    if token_are_connected {
                        return Ok(Ordering::Less);
                    } else if self.gs_order.is_connected(
                        m2_lefttok,
                        m1_lefttok,
                        1,
                        std::ops::Bound::Unbounded,
                    )? {
                        return Ok(Ordering::Greater);
                    }
                }

                // 3. compare the name
                let name_cmp = m1_name.cmp(m2_name);
                if name_cmp != Ordering::Equal {
                    return Ok(name_cmp);
                }
            }

            // compare node IDs directly as last resort
            Ok(m1.node.cmp(&m2.node))
        }
    }
}

fn split_path_and_nodename(full_node_name: &str) -> (&str, &str) {
    full_node_name
        .rsplit_once('#')
        .unwrap_or((full_node_name, ""))
}

fn compare_document_path(p1: &str, p2: &str) -> std::cmp::Ordering {
    let it1 = p1.split('/').filter(|s| !s.is_empty());
    let it2 = p2.split('/').filter(|s| !s.is_empty());

    for (part1, part2) in it1.zip(it2) {
        let string_cmp = part1.cmp(part2);
        if string_cmp != std::cmp::Ordering::Equal {
            return string_cmp;
        }
    }

    // Both paths have the same prefix, check if one of them has more elements.
    // TODO: Since both iterators have been moved, they have to be recreated, there
    // should be a more efficient way of doing this.
    let length1 = p1.split('/').filter(|s| !s.is_empty()).count();
    let length2 = p2.split('/').filter(|s| !s.is_empty()).count();
    length1.cmp(&length2)
}

#[cfg(test)]
mod tests {

    use std::{io::BufReader, path::Path};

    use graphannis::model::{AnnotationComponent, AnnotationComponentType};
    use graphannis_core::graph::{ANNIS_NS, NODE_TYPE_KEY, serialization::graphml};

    use super::*;

    #[test]
    fn tiger_doc_name_sort() {
        let p1 = "tiger2/tiger2/tiger_release_dec05_110";
        let p2 = "tiger2/tiger2/tiger_release_dec05_1_1";
        assert_eq!(std::cmp::Ordering::Less, compare_document_path(p1, p2));
    }

    #[test]
    fn compare_match_for_example_graph() {
        let input_file = std::fs::File::open(Path::new(
            "tests/data/import/graphml/single_sentence.graphml",
        ))
        .unwrap();
        let input_file = BufReader::new(input_file);
        let (graph, _) =
            graphml::import::<AnnotationComponentType, _, _>(input_file, false, |_| {}).unwrap();

        let gs_order = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "".into(),
            ))
            .unwrap();
        let token_helper = TokenHelper::new(&graph).unwrap();

        let t3_id = graph
            .get_node_annos()
            .get_node_id_from_name("single_sentence/zossen#t3")
            .unwrap()
            .unwrap();
        let t5_id = graph
            .get_node_annos()
            .get_node_id_from_name("single_sentence/zossen#t5")
            .unwrap()
            .unwrap();

        let mut sort_cache = SortCache::new(gs_order);

        // Test same node should be equal
        let match_t3 = Match {
            node: t3_id,
            anno_key: NODE_TYPE_KEY.clone(),
        };
        assert_eq!(
            Ordering::Equal,
            sort_cache
                .compare_match_by_text_pos(
                    &match_t3,
                    &match_t3,
                    graph.get_node_annos(),
                    &token_helper
                )
                .unwrap()
        );

        // t5 comes after
        let match_t5 = Match {
            node: t5_id,
            anno_key: NODE_TYPE_KEY.clone(),
        };
        assert_eq!(
            Ordering::Less,
            sort_cache
                .compare_match_by_text_pos(
                    &match_t3,
                    &match_t5,
                    graph.get_node_annos(),
                    &token_helper
                )
                .unwrap()
        );
        assert_eq!(
            Ordering::Greater,
            sort_cache
                .compare_match_by_text_pos(
                    &match_t5,
                    &match_t3,
                    graph.get_node_annos(),
                    &token_helper
                )
                .unwrap()
        );
    }
}
