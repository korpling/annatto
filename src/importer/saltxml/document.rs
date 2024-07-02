use std::{collections::BTreeMap, convert::TryFrom};

use anyhow::{anyhow, Context, Result};
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;
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
            let element_id =
                get_element_id(&text_node).context("Missing element ID for textual data source")?;

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
                let id = get_element_id(&t).context("Missing element ID for token source")?;
                Ok((*t, id))
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
            let source_att_val = text_rel.attribute("source").unwrap_or_default();
            let token =
                resolve_element(source_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation source \"{source_att_val}\" could not be resolved")
                })?;

            let target_att_val = text_rel.attribute("target").unwrap_or_default();
            let datasource =
                resolve_element(target_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation target \"{target_att_val}\" could not be resolved")
                })?;
            let token_id = get_element_id(&token).context("Missing ID for token")?;
            let datasource_id = get_element_id(&datasource).context("Missing ID for token")?;

            // Get the string for this token
            let matching_base_text = self
                .base_texts
                .get(&datasource_id)
                .with_context(|| format!("Missing base text for token {token_id}"))?;
            let start_offset =
                get_feature_by_qname(text_rel, "salt", "SSTART").context("Missing start value")?;
            let end_offset =
                get_feature_by_qname(text_rel, "salt", "SEND").context("Missing end value")?;
            if let (SaltObject::Integer(start), SaltObject::Integer(end)) =
                (start_offset, end_offset)
            {
                let start = usize::try_from(start)?;
                let end = usize::try_from(end)?;
                let covered_text = &matching_base_text[start..end];
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name: token_id,
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: covered_text.to_string(),
                })?;
            }

            // TODO also get whitespace after/before
        }

        Ok(())
    }
}
