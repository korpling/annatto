use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use graphannis::update::{GraphUpdate, UpdateEvent};
use itertools::Itertools;
use roxmltree::Node;

use super::{get_element_id, get_feature_by_qname, resolve_element, SaltObject, SaltType};

pub(super) struct DocumentMapper<'a, 'input> {
    nodes: Vec<Node<'a, 'input>>,
    edges: Vec<Node<'a, 'input>>,
    layers: Vec<Node<'a, 'input>>,
    base_texts: BTreeMap<String, String>,
}

impl<'a, 'input> DocumentMapper<'a, 'input> {
    pub(super) fn read_document(
        input: &'input str,
        _document_node_name: &str,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let doc = roxmltree::Document::parse(input)?;
        let root = doc.root_element();
        if root.tag_name().name() != "SDocumentGraph" {
            return Err(anyhow!(
                "SaltXML document file must start with <SDocumentGraph> tag"
            ));
        }

        let nodes = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "nodes")
            .collect_vec();

        let edges = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "edges")
            .collect_vec();

        let layers = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "layers")
            .collect_vec();
        let mut mapper = DocumentMapper {
            base_texts: BTreeMap::new(),
            nodes,
            edges,
            layers,
        };
        mapper.map_textual_ds(updates)?;
        mapper.map_token(updates)?;

        Ok(())
    }

    fn map_textual_ds(&mut self, updates: &mut GraphUpdate) -> Result<()> {
        for text_node in self
            .nodes
            .iter()
            .filter(|n| SaltType::from(**n) == SaltType::TextualDs)
        {
            let element_id = get_element_id(&text_node)
                .ok_or_else(|| anyhow!("Missing element ID for textual data source"))?;

            if let Some(SaltObject::Text(anno_value)) =
                get_feature_by_qname(&text_node, "saltCommon", "SDATA")
            {
                self.base_texts.insert(element_id.clone(), anno_value);
                updates.add_event(UpdateEvent::AddNode {
                    node_name: element_id.clone(),
                    node_type: "datasource".to_string(),
                })?;
            }
        }
        Ok(())
    }

    fn map_token(&self, updates: &mut GraphUpdate) -> Result<()> {
        // Get the list of token in the same order as in the SaltXML file
        let tokens: Result<Vec<_>> = self
            .nodes
            .iter()
            .filter(|n| SaltType::from(**n) == SaltType::Token)
            .map(|t| {
                let id = get_element_id(&t)
                    .ok_or_else(|| anyhow!("Missing element ID for token source"))?;
                Ok((t, id))
            })
            .collect();
        let tokens = tokens?;

        for (_, t_id) in tokens.iter() {
            updates.add_event(UpdateEvent::AddNode {
                node_name: t_id.clone(),
                node_type: "node".to_string(),
            })?;
        }
        // Connect the token to the texts by the textual relations
        for text_rel in self
            .edges
            .iter()
            .filter(|n| SaltType::from(**n) == SaltType::TextualRelation)
        {
            let token = resolve_element(
                text_rel.attribute("source").unwrap_or_default(),
                "nodes",
                &self.nodes,
            );
            let datasource = resolve_element(
                text_rel.attribute("target").unwrap_or_default(),
                "nodes",
                &self.nodes,
            );
            if let (Some(_token), Some(_datasource)) = (token, datasource) {}
        }

        Ok(())
    }
}
